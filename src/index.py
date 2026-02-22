from workers import WorkerEntrypoint, Response
from pyodide.ffi import to_js
from urllib.parse import urlparse, parse_qs, quote
import js, json, hashlib, hmac, traceback, asyncio
from datetime import datetime, timezone

# ==========================================
# CONSTANTS & CONFIGURATIONS
# ==========================================
MAX_IPS_PER_RECORD = 5
HW_LINES_FALLBACK = {"CM": "Yidong", "CU": "Liantong", "CT": "Dianxin"}

class Default(WorkerEntrypoint):
    def get_env_var(self, key, default=""):
        try:
            val = getattr(self.env, key, None)
            return default if val is None or str(val).strip() in ["undefined", ""] else str(val).strip()
        except: 
            return default

    async def fetch(self, request):
        try:
            url_obj = urlparse(str(request.url))
            path = url_obj.path
            query_params = parse_qs(url_obj.query)

            # -----------------------------------------------------------
            # REST API ROUTE: BACKGROUND SYNC EXECUTION
            # -----------------------------------------------------------
            if path == "/api/sync":
                if request.method != "POST":
                    return Response("Method Not Allowed", status=405)

                expected_token = self.get_env_var("SYNC_TOKEN")
                try:
                    req_data = await request.json()
                    provided_token = req_data.get("token")
                except:
                    provided_token = ""

                if not expected_token or provided_token != expected_token:
                    return Response("Unauthorized Execution Attempt", status=401)

                sync_logs = []
                sync_logs.append(f"[{datetime.now().isoformat()}] [INFO]  [INIT] Background synchronization sequence triggered via API.")
                try:
                    await self.perform_full_sync(sync_logs)
                except Exception as e:
                    sync_logs.append(f"[{datetime.now().isoformat()}] [FATAL] [EXECUTION] {traceback.format_exc()}")

                return Response("\n".join(sync_logs), status=200, headers={"Content-Type": "text/plain;charset=UTF-8"})

            # -----------------------------------------------------------
            # FRONTEND ROUTING & AUTHENTICATION
            # -----------------------------------------------------------
            is_sync_triggered = False
            token_to_pass = ""

            if path == "/sync":
                expected_token = self.get_env_var("SYNC_TOKEN")
                provided_token = query_params.get("token", [""])[0]

                if not expected_token or provided_token != expected_token:
                    return Response("", status=302, headers={"Location": "/"})
                
                is_sync_triggered = True
                token_to_pass = provided_token
            elif path != "/":
                return Response("", status=302, headers={"Location": "/"})

            # -----------------------------------------------------------
            # CLIENT CONTEXT EXTRACTION
            # -----------------------------------------------------------
            rid = str(request.headers.get('cf-ray') or 'Unknown')
            cip = str(request.headers.get('cf-connecting-ip') or 'Unknown')
            
            js_req = getattr(request, "js_object", None)
            cf_obj = getattr(js_req, "cf", None) if js_req else None
            
            def get_cf(key, default="Unknown"):
                if not cf_obj: return default
                try:
                    val = getattr(cf_obj, key, default)
                    return default if val is None or str(val).strip() in ("", "undefined", "None") else str(val)
                except Exception:
                    return default
                    
            cnt = str(request.headers.get('cf-ipcountry') or get_cf('country'))
            reg = get_cf('region')
            cty = get_cf('city')
            clo = get_cf('colo')
            prt = get_cf('httpProtocol', 'HTTP')
            tls = get_cf('tlsVersion')
            
            hst = urlparse(str(request.url)).hostname or "Unknown"
            raw_parts = [p for p in (cty, reg, cnt) if p and p != "Unknown"]
            loc_str = ", ".join(list(dict.fromkeys(raw_parts))) if raw_parts else "Unknown"

            music_json_url = self.get_env_var("MUSIC_JSON_URL", "")
            player_visibility_class = "" if music_json_url else "hide-player"

            # -----------------------------------------------------------
            # HTML TEMPLATE RENDERING
            # -----------------------------------------------------------
            tpl = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>Node Diagnostics - ${currentHost}</title>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
    <style>
        body, html { margin: 0; padding: 0; height: 100%; width: 100%; background: #0d0d0d; font-family: 'Consolas', 'Monaco', 'Microsoft YaHei', sans-serif; display: flex; justify-content: center; align-items: center; overflow: hidden; }
        .bg-overlay { position: fixed; top: 0; left: 0; right: 0; bottom: 0; width: 100%; height: 100%; object-fit: cover; z-index: 0; opacity: 0; transition: opacity 1.5s cubic-bezier(0.19, 1, 0.22, 1); }
        .bg-overlay.loaded { opacity: 1; }
        .bg-dimmer { position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0, 0, 0, 0.65); z-index: 1; }
        .glass-container { position: relative; z-index: 10; background: rgba(20, 20, 20, 0.55); backdrop-filter: blur(25px) saturate(120%); -webkit-backdrop-filter: blur(25px) saturate(120%); border: 1px solid rgba(255, 255, 255, 0.15); border-radius: 8px; padding: 25px; color: #d0d0d0; width: 85%; max-width: 400px; box-shadow: 0 20px 40px rgba(0, 0, 0, 0.7); }
        .terminal-header { border-bottom: 1px solid rgba(255, 255, 255, 0.1); padding-bottom: 12px; margin-bottom: 15px; display: flex; justify-content: space-between; align-items: center; }
        .header-left { display: flex; align-items: center; gap: 10px; flex: 1; min-width: 0; margin-right: 10px; }
        .header-right { display: flex; align-items: center; gap: 8px; flex-shrink: 0; }
        h1 { font-size: 1.1rem; margin: 0; color: #fff; font-family: 'Consolas', monospace; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 100%; }
        .badge { color: #00ff88; padding: 2px 8px; border-radius: 3px; font-size: 0.7rem; border: 1px solid #00ff88; font-weight: bold; margin-left: 5px;}
        .action-btn { background: rgba(255, 255, 255, 0.1); border: 1px solid rgba(255, 255, 255, 0.2); color: #fff; width: 28px; height: 28px; border-radius: 6px; cursor: pointer; transition: all 0.3s ease; display: flex; justify-content: center; align-items: center; }
        .action-btn:hover { background: rgba(255, 255, 255, 0.25); transform: translateY(-2px); box-shadow: 0 5px 10px rgba(0,0,0,0.3); }
        .tech-desc { font-size: 0.8rem; line-height: 1.5; margin-bottom: 15px; color: #999; background: rgba(0, 0, 0, 0.25); padding: 12px; border-left: 2px solid #555; }
        .info-grid { display: grid; gap: 8px; }
        .info-item { display: flex; justify-content: space-between; align-items: flex-start; font-size: 0.8rem; padding: 4px 0; border-bottom: 1px solid rgba(255,255,255,0.05); }
        .info-item .key { color: #666; flex-shrink: 0; width: 85px; padding-top: 2px; }
        .info-item .val { color: #00d2ff; font-weight: bold; text-align: right; word-break: break-all; white-space: normal; line-height: 1.3; flex-grow: 1; }
        .footer { margin-top: 25px; text-align: center; font-size: 0.7rem; color: #555; }
        .footer a { color: #ff758c; text-decoration: none; font-weight: bold; transition: all 0.3s ease; }
        .footer a:hover { color: #ff1493; text-shadow: 0 0 10px rgba(255, 20, 147, 0.8); }
        .footer a.cf-link { color: #F38020; }
        .footer a.cf-link:hover { color: #FF9933; text-shadow: 0 0 10px rgba(243, 128, 32, 0.8); }
        #l2d-canvas { position: fixed; bottom: -10px; left: -15px; z-index: 999999 !important; pointer-events: none; }
        
        .music-player { position: fixed; bottom: 30px; right: 30px; background: rgba(20, 20, 20, 0.6); backdrop-filter: blur(15px); -webkit-backdrop-filter: blur(15px); border: 1px solid rgba(255, 255, 255, 0.15); border-radius: 50px; padding: 6px 16px 6px 6px; display: flex; align-items: center; gap: 10px; z-index: 100; box-shadow: 0 10px 25px rgba(0,0,0,0.5); transition: all 0.4s cubic-bezier(0.175, 0.885, 0.32, 1.275); opacity: 0; visibility: hidden; }
        .music-player.active { opacity: 1; visibility: visible; }
        .hide-player .music-player { display: none !important; }
        
        .cover-art { width: 42px; height: 42px; border-radius: 50%; object-fit: cover; animation: spin 6s linear infinite; animation-play-state: paused; border: 2px solid rgba(255, 255, 255, 0.1); }
        .cover-art.playing { animation-play-state: running; }
        @keyframes spin { 100% { transform: rotate(360deg); } }
        
        .track-info { display: flex; flex-direction: column; justify-content: center; width: 100px; }
        .track-title { color: #fff; font-size: 0.8rem; font-weight: bold; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        .track-artist { color: #888; font-size: 0.6rem; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        
        .player-controls { display: flex; align-items: center; gap: 8px; }
        .ctrl-btn { background: none; border: none; color: rgba(255,255,255,0.4); cursor: pointer; font-size: 0.8rem; transition: all 0.2s; padding: 0; display: flex; align-items: center; justify-content: center; }
        .ctrl-btn:hover { color: #fff; transform: scale(1.1); }
        .play-btn { background: rgba(0, 210, 255, 0.15); border: 1px solid rgba(0, 210, 255, 0.4); color: #00d2ff; width: 32px; height: 32px; border-radius: 50%; font-size: 0.7rem; }
        .play-btn.playing { color: #ff758c; border-color: rgba(255, 117, 140, 0.5); background: rgba(255, 117, 140, 0.15); }

        .glass-container, .bg-dimmer, #l2d-canvas, .music-player { transition: opacity 0.5s ease, visibility 0.5s, transform 0.5s ease; }
        body.interface-hidden .glass-container, body.interface-hidden .bg-dimmer, body.interface-hidden #l2d-canvas, body.interface-hidden .music-player { opacity: 0 !important; visibility: hidden !important; pointer-events: none !important; transform: translateY(20px); }
        
        .hidden-controls { position: fixed; bottom: 40px; left: 50%; transform: translateX(-50%); display: flex; gap: 15px; z-index: 999999; opacity: 0; pointer-events: none; transition: all 0.4s ease; }
        body.interface-hidden .hidden-controls { opacity: 1; pointer-events: auto; }
        .control-btn { background: rgba(0, 0, 0, 0.6); backdrop-filter: blur(10px); color: #fff; padding: 10px 20px; border-radius: 30px; border: 1px solid rgba(255, 255, 255, 0.2); cursor: pointer; font-size: 0.9rem; font-weight: bold; display: flex; align-items: center; gap: 8px; transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1); }
        .control-btn:hover { background: rgba(255, 255, 255, 0.15); border-color: rgba(255, 255, 255, 0.4); transform: translateY(-3px); box-shadow: 0 8px 15px rgba(0, 0, 0, 0.4); }
        .control-btn:active { transform: translateY(-1px); box-shadow: 0 4px 8px rgba(0, 0, 0, 0.3); }        .toast { position: fixed; top: 30px; left: 50%; transform: translate(-50%, -20px); background: rgba(255, 117, 140, 0.95); color: #fff; padding: 12px 24px; border-radius: 8px; font-size: 0.85rem; font-weight: bold; z-index: 1000000; opacity: 0; transition: all 0.4s; text-align: center; }
        .toast.show { opacity: 1; transform: translate(-50%, 0); }
        @media (max-width: 768px) { .glass-container { padding: 20px; width: 76%; } .music-player { bottom: 20px; right: 20px; } }
    </style>
</head>
<body class="${playerVisibilityClass}">
    <img class="bg-overlay" id="dynamic-bg" alt="Background">
    <div class="bg-dimmer"></div>
    <div class="glass-container">
        <div class="terminal-header">
            <div class="header-left">
                <h1><i class="fas fa-network-wired"></i> ${currentHost.toUpperCase()}</h1>
            </div>
            <div class="header-right">
                <button class="action-btn" onclick="toggleInterface()" title="Hide Interface"><i class="fas fa-eye-slash"></i></button>
                <div class="badge">RUNNING</div>
            </div>
        </div>
        <div class="tech-desc">
            <b>Target Usage:</b> This domain operates exclusively as a dynamic CNAME target for Mainland China network optimization. DNS records are synced automatically every 15 minutes.
        </div>
        <div class="info-grid">
            <div class="info-item"><span class="key">Client_IP</span><span class="val">${clientIp}</span></div>
            <div class="info-item"><span class="key">Ray_ID</span><span class="val">${rayId}</span></div>
            <div class="info-item"><span class="key">Node</span><span class="val">${colo}</span></div>
            <div class="info-item"><span class="key">Region</span><span class="val">${location}</span></div>
            <div class="info-item"><span class="key">Protocol</span><span class="val">${httpProtocol} / ${tlsVersion}</span></div>
        </div>
        <div class="footer">
            Powered by <a href="https://workers.cloudflare.com/" target="_blank" class="cf-link">Cloudflare Workers</a><br>
            <div style="margin-top: 8px;">
                &copy; <span id="current-year"></span> <a href="https://github.com/racpast" target="_blank">RACPAST</a>. ALL RIGHTS RESERVED.
            </div>
        </div>
    </div>

    <div class="music-player" id="music-player">
        <img src="" alt="Cover" class="cover-art" id="cover-art">
        <div class="track-info">
            <div class="track-title" id="track-title">Loading...</div>
            <div class="track-artist" id="track-artist">...</div>
        </div>
        <div class="player-controls">
            <button class="ctrl-btn" id="prev-btn"><i class="fas fa-step-backward"></i></button>
            <button class="ctrl-btn play-btn" id="play-btn"><i class="fas fa-play"></i></button>
            <button class="ctrl-btn" id="next-btn"><i class="fas fa-step-forward"></i></button>
        </div>
        <audio id="bg-audio"></audio>
    </div>

    <div class="hidden-controls">
        <button class="control-btn" onclick="refreshBg()"><i class="fas fa-sync-alt"></i> Refresh</button>
        <button class="control-btn" onclick="toggleInterface()"><i class="fas fa-sign-out-alt"></i> Exit</button>
    </div>
    <div class="toast" id="cute-toast"></div>
    <canvas id="l2d-canvas" width="280" height="320"></canvas>
    
    <script src="https://fastly.jsdelivr.net/gh/stevenjoezhang/live2d-widget@latest/live2d.min.js"></script>
    <script>
        document.getElementById('current-year').textContent = new Date().getFullYear();
        const isPortrait = window.matchMedia("(orientation: portrait)").matches;
        const bgImg = document.getElementById('dynamic-bg');
        function loadBg() {
            const baseUrl = isPortrait ? 'https://www.loliapi.com/acg/pe/' : 'https://www.loliapi.com/acg/pc/';
            const targetUrl = baseUrl + '?t=' + new Date().getTime();
            const tempImg = new Image();
            tempImg.onload = () => {
                bgImg.src = targetUrl;
                requestAnimationFrame(() => {
                    bgImg.classList.add('loaded');
                });
            };
            tempImg.src = targetUrl;
        }
        loadBg();

        function toggleInterface() {
            const isHidden = document.body.classList.toggle('interface-hidden');
            if (isHidden) {
                const toast = document.getElementById('cute-toast');
                toast.innerHTML = '<i class="fas fa-info-circle"></i> Interface hidden. Use controls to return.';
                toast.classList.add('show');
                setTimeout(() => toast.classList.remove('show'), 3500);
            }
        }
        function refreshBg() { 
            bgImg.classList.remove('loaded');
            setTimeout(loadBg, 500); 
        }

        const musicJsonUrl = "${musicJsonUrl}";
        const playerEl = document.getElementById('music-player');
        const audio = document.getElementById('bg-audio');
        const playBtn = document.getElementById('play-btn');
        const coverArt = document.getElementById('cover-art');
        const trackTitle = document.getElementById('track-title');
        const trackArtist = document.getElementById('track-artist');
        
        let playlist = [];
        let curIndex = 0;

        if (musicJsonUrl && !document.body.classList.contains('hide-player')) {
            fetch(musicJsonUrl)
                .then(r => r.json())
                .then(data => {
                    if (data && data.length > 0) {
                        playlist = data;
                        curIndex = Math.floor(Math.random() * playlist.length);
                        loadTrack(curIndex);
                        playerEl.classList.add('active');
                    }
                })
                .catch(e => console.error("Playlist failed:", e));
        }

        function loadTrack(index) {
            const t = playlist[index];
            audio.src = t.audio;
            coverArt.src = t.cover;
            trackTitle.innerText = t.title;
            trackArtist.innerText = t.artist;
            if (!audio.paused) audio.play();
        }

        function togglePlay() {
            if (audio.paused) {
                audio.play().then(() => {
                    playBtn.innerHTML = '<i class="fas fa-pause"></i>';
                    playBtn.classList.add('playing');
                    coverArt.classList.add('playing');
                });
            } else {
                audio.pause();
                playBtn.innerHTML = '<i class="fas fa-play"></i>';
                playBtn.classList.remove('playing');
                coverArt.classList.remove('playing');
            }
        }

        playBtn.addEventListener('click', togglePlay);
        document.getElementById('next-btn').addEventListener('click', () => {
            curIndex = (curIndex + 1) % playlist.length;
            loadTrack(curIndex);
            audio.play();
            playBtn.innerHTML = '<i class="fas fa-pause"></i>';
            playBtn.classList.add('playing');
            coverArt.classList.add('playing');
        });
        document.getElementById('prev-btn').addEventListener('click', () => {
            curIndex = (curIndex - 1 + playlist.length) % playlist.length;
            loadTrack(curIndex);
            audio.play();
            playBtn.innerHTML = '<i class="fas fa-pause"></i>';
            playBtn.classList.add('playing');
            coverArt.classList.add('playing');
        });
        audio.addEventListener('ended', () => {
            curIndex = (curIndex + 1) % playlist.length;
            loadTrack(curIndex);
            audio.play();
        });

        // Live2D
        try { loadlive2d("l2d-canvas", "https://live2d.fghrsh.net/api/get/?id=2-0"); } catch(e) {}
    </script>
</body>
</html>
"""
            r = tpl.replace("${currentHost}", hst) \
                   .replace("${currentHost.toUpperCase()}", hst.upper()) \
                   .replace("${clientIp}", cip) \
                   .replace("${rayId}", rid) \
                   .replace("${colo}", clo) \
                   .replace("${location}", loc_str) \
                   .replace("${httpProtocol}", prt) \
                   .replace("${tlsVersion}", tls) \
                   .replace("${musicJsonUrl}", music_json_url) \
                   .replace("${playerVisibilityClass}", player_visibility_class)
                   
            if is_sync_triggered:
                sync_script = f"""
                <script>
                    window.history.replaceState({{}}, document.title, "/");
                    setTimeout(() => {{
                        const toast = document.getElementById('cute-toast');
                        toast.innerHTML = '<i class="fas fa-terminal"></i> Background synchronization in progress. Open Console (F12) to view logs.';
                        toast.classList.add('show');
                        setTimeout(() => toast.classList.remove('show'), 5000);
                    }}, 300);

                    console.log("[%cSYS%c] Handshake accepted. Dispatching background synchronization...", "color:#00ff88;font-weight:bold;", "color:inherit;");
                    
                    fetch('/api/sync', {{
                        method: 'POST',
                        headers: {{ 'Content-Type': 'application/json' }},
                        body: JSON.stringify({{ token: "{token_to_pass}" }})
                    }}).then(response => response.text()).then(logs => {{
                        console.log("\\n=== HW CLOUD DNS RECONCILIATION TELEMETRY ===\\n" + logs + "\\n==============================================");
                        console.log("[%cOK%c] Remote state fully synchronized.", "color:#00d2ff;font-weight:bold;", "color:inherit;");
                    }}).catch(err => {{
                        console.error("[%cERR%c] Sync API communication failed:", "color:#ff758c;font-weight:bold;", "color:inherit;", err);
                    }});
                </script>
                """
                r = r.replace("</body>", f"{sync_script}</body>")

            return Response(r, headers={"content-type": "text/html;charset=UTF-8"})
            
        except Exception as e:
            return Response(f"Internal Worker Execution Error: {str(e)}", status=500)

    # -----------------------------------------------------------
    # CRON JOB EXECUTION
    # -----------------------------------------------------------
    async def scheduled(self, event, env, ctx):
        try:
            logs = [f"[{datetime.now().isoformat()}] [INFO]  [CRON] Scheduled reconciliation sequence initiated."]
            await self.perform_full_sync(logs)
            print("\n".join(logs))
        except Exception as e:
            print(f"[FATAL] [CRON] Execution aborted: {str(e)}")

    # ===========================================================
    # CORE DNS SYNCHRONIZATION ENGINE (HUAWEI CLOUD)
    # ===========================================================
    def extract_ips(self, info_dict, key):
        if not info_dict or not info_dict.get(key):
            return []
        return list(dict.fromkeys([item['ip'] for item in info_dict[key]]))

    async def perform_full_sync(self, logs):
        logs.append(f"[{datetime.now().isoformat()}] [INFO]  [UPSTREAM] Fetching optimal IPs from provider...")
        v4_info = await self.get_wetest_ips("v4", logs)
        v6_info = await self.get_wetest_ips("v6", logs)
        if not v4_info and not v6_info:
            logs.append(f"[{datetime.now().isoformat()}] [ERROR] [UPSTREAM] Upstream payload empty. Halting operation.")
            return

        domain_name = self.get_env_var("DOMAIN_NAME", "cdn.rpnet.cc")
        sub_domain = self.get_env_var("SUB_DOMAIN", "@")
        full_hostname = f"{domain_name}." if sub_domain == "@" else f"{sub_domain}.{domain_name}."

        zone_id = self.get_env_var("HW_ZONE_ID")
        region = self.get_env_var("HW_REGION", "cn-east-3")
        host = f"dns.{region}.myhuaweicloud.com"

        logs.append(f"[{datetime.now().isoformat()}] [INFO]  [HW_API] Compiling HW system routing topology.")
        system_lines = await self.get_system_lines(host, zone_id, logs)
        name_to_id = {line['name']: line['id'] for line in system_lines if line.get('name') and line.get('id')}

        line_map = {}
        for net_code, chinese_name in [("CM", "移动"), ("CU", "联通"), ("CT", "电信")]:
            line_map[net_code] = name_to_id.get(chinese_name, HW_LINES_FALLBACK[net_code])

        logs.append(f"[{datetime.now().isoformat()}] [INFO]  [STATE] Modeling target DNS record definitions.")
        target_v4 = {}
        target_v6 = {}

        v4_cn = self.extract_ips(v4_info, "CN")
        if not v4_cn:
            v4_cn = list(dict.fromkeys(self.extract_ips(v4_info, "CM") + self.extract_ips(v4_info, "CU") + self.extract_ips(v4_info, "CT")))
        if v4_cn: target_v4["default_view"] = v4_cn[:MAX_IPS_PER_RECORD]

        v6_cn = self.extract_ips(v6_info, "CN")
        if not v6_cn:
            v6_cn = list(dict.fromkeys(self.extract_ips(v6_info, "CM") + self.extract_ips(v6_info, "CU") + self.extract_ips(v6_info, "CT")))
        if v6_cn: target_v6["default_view"] = v6_cn[:MAX_IPS_PER_RECORD]

        for net_code, hw_line in line_map.items():
            ips_v4 = self.extract_ips(v4_info, net_code)[:MAX_IPS_PER_RECORD]
            ips_v6 = self.extract_ips(v6_info, net_code)[:MAX_IPS_PER_RECORD]
            if ips_v4: target_v4[hw_line] = ips_v4
            if ips_v6: target_v6[hw_line] = ips_v6

        logs.append(f"[{datetime.now().isoformat()}] [INFO]  [HW_API] Querying authoritative remote state.")
        existing_records = await self.get_hw_recordsets(host, zone_id, full_hostname, logs)
        ttl = 600

        for rec in existing_records:
            rec_id = rec["id"]
            line = rec["line"]
            rtype = rec["type"]
            
            if rtype == "A":
                if line in target_v4:
                    logs.append(f"[{datetime.now().isoformat()}] [WARN]  [STATE] Desynchronization detected on A ({line}). Commencing update.")
                    await self.update_hw_record(rec_id, full_hostname, target_v4[line], "A", ttl, logs)
                    del target_v4[line]
                else:
                    logs.append(f"[{datetime.now().isoformat()}] [WARN]  [GARBAGE] Orphaned record A ({line}) isolated. Purging.")
                    await self.delete_hw_record(host, zone_id, rec_id, logs)
            
            elif rtype == "AAAA":
                if line in target_v6:
                    logs.append(f"[{datetime.now().isoformat()}] [WARN]  [STATE] Desynchronization detected on AAAA ({line}). Commencing update.")
                    await self.update_hw_record(rec_id, full_hostname, target_v6[line], "AAAA", ttl, logs)
                    del target_v6[line]
                else:
                    logs.append(f"[{datetime.now().isoformat()}] [WARN]  [GARBAGE] Orphaned record AAAA ({line}) isolated. Purging.")
                    await self.delete_hw_record(host, zone_id, rec_id, logs)

        for line, ips in target_v4.items():
            logs.append(f"[{datetime.now().isoformat()}] [INFO]  [STATE] Provisioning missing record A ({line}).")
            await self.create_hw_record(host, zone_id, full_hostname, "A", ips, line, ttl, logs)
            
        for line, ips in target_v6.items():
            logs.append(f"[{datetime.now().isoformat()}] [INFO]  [STATE] Provisioning missing record AAAA ({line}).")
            await self.create_hw_record(host, zone_id, full_hostname, "AAAA", ips, line, ttl, logs)

        logs.append(f"[{datetime.now().isoformat()}] [INFO]  [SUCCESS] System infrastructure strictly synchronized.")

    # -----------------------------------------------------------
    # UPSTREAM & HW CLOUD API METHODS
    # -----------------------------------------------------------
    async def get_wetest_ips(self, ip_type, logs):
        url = f"https://www.wetest.vip/api/cf2dns/get_cloudflare_ip?key={self.get_env_var('OPTIMIZE_KEY', 'o1zrmHAF')}&type={ip_type}"
        try:
            resp = await js.fetch(url)
            data = json.loads(await resp.text())
            if data.get("status") and data.get("code") == 200:
                return data.get("info")
            else:
                logs.append(f"[{datetime.now().isoformat()}] [WARN]  [UPSTREAM] Extraction failed for {ip_type}: {data.get('msg')}")
                return None
        except Exception as e:
            logs.append(f"[{datetime.now().isoformat()}] [ERROR] [UPSTREAM] Fetch exception on {ip_type}: {str(e)}")
            return None

    async def get_system_lines(self, host, zone_id, logs):
        url = f"https://{host}/v2.1/system-lines"
        try:
            headers = self.hw_sign("GET", url, "", host)
            resp = await js.fetch(url, to_js({"method": "GET", "headers": headers}, dict_converter=js.Object.fromEntries))
            data = json.loads(await resp.text())
            return data.get('lines', [])
        except Exception as e:
            logs.append(f"[{datetime.now().isoformat()}] [ERROR] [HW_API] System topology query aborted: {str(e)}")
            return []

    async def get_hw_recordsets(self, host, zone_id, hostname, logs):
        url = f"https://{host}/v2.1/zones/{zone_id}/recordsets?name={hostname}"
        records = []
        try:
            headers = self.hw_sign("GET", url, "", host)
            resp = await js.fetch(url, to_js({"method": "GET", "headers": headers}, dict_converter=js.Object.fromEntries))
            data_py = json.loads(await resp.text())
            for r in data_py.get('recordsets', []):
                rtype = r.get('type')
                if rtype in ('A', 'AAAA'):
                    line_val = r.get('line')
                    if not line_val or line_val == 'None':
                        line_val = 'default_view'
                    records.append({"id": r.get('id'), "line": line_val, "type": rtype, "records": r.get('records', [])})
            return records
        except Exception as e:
            logs.append(f"[{datetime.now().isoformat()}] [ERROR] [HW_API] Authority record retrieval failed: {str(e)}")
            return []

    async def delete_hw_record(self, host, zone_id, record_id, logs):
        url = f"https://{host}/v2.1/zones/{zone_id}/recordsets/{record_id}"
        try:
            headers = self.hw_sign("DELETE", url, "", host)
            resp = await js.fetch(url, to_js({"method": "DELETE", "headers": headers}, dict_converter=js.Object.fromEntries))
            if not resp.ok: logs.append(f"[{datetime.now().isoformat()}] [ERROR] [HW_API] Deletion rejected: HTTP {resp.status}")
        except Exception as e:
            logs.append(f"[{datetime.now().isoformat()}] [ERROR] [HW_API] Exception during deletion: {str(e)}")

    async def create_hw_record(self, host, zone_id, name, record_type, ips, line, ttl, logs):
        url = f"https://{host}/v2.1/zones/{zone_id}/recordsets"
        body = json.dumps({"name": name, "type": record_type, "records": ips, "line": line, "ttl": ttl})
        try:
            headers = self.hw_sign("POST", url, body, host)
            resp = await js.fetch(url, to_js({"method": "POST", "headers": headers, "body": body}, dict_converter=js.Object.fromEntries))
            if not resp.ok: logs.append(f"[{datetime.now().isoformat()}] [ERROR] [HW_API] Provisioning rejected: {await resp.text()}")
        except Exception as e:
            logs.append(f"[{datetime.now().isoformat()}] [ERROR] [HW_API] Exception during provisioning: {str(e)}")

    async def update_hw_record(self, record_id, hostname, ips, record_type, ttl, logs):
        zone_id = self.get_env_var("HW_ZONE_ID")
        region = self.get_env_var("HW_REGION", "cn-east-3")
        host = f"dns.{region}.myhuaweicloud.com"
        url = f"https://{host}/v2.1/zones/{zone_id}/recordsets/{record_id}"
        body = json.dumps({"name": hostname, "type": record_type, "records": ips, "ttl": ttl})
        headers = self.hw_sign("PUT", url, body, host)
        try:
            resp = await js.fetch(url, to_js({"method": "PUT", "headers": headers, "body": body}, dict_converter=js.Object.fromEntries))
            if not resp.ok: logs.append(f"[{datetime.now().isoformat()}] [ERROR] [HW_API] Update rejected: {await resp.text()}")
        except Exception as e:
            logs.append(f"[{datetime.now().isoformat()}] [ERROR] [HW_API] Exception during update: {str(e)}")

    def hw_sign(self, method, url, body, host):
        ak, sk = self.get_env_var("HW_AK"), self.get_env_var("HW_SK")
        path_query = url.split(host)[1]
        uri = path_query.split('?')[0]
        if not uri.endswith('/'): uri += '/'
        query_str = path_query.split('?')[1] if '?' in path_query else ""
        canonical_qs = ""
        if query_str:
            q_pairs = [f"{quote(k, safe='~')}={quote(v, safe='~')}" for param in query_str.split('&') for k, v in [param.split('=', 1) if '=' in param else (param, "")]]
            q_pairs.sort()
            canonical_qs = '&'.join(q_pairs)
        t = datetime.now(timezone.utc)
        sdk_date = t.strftime('%Y%m%dT%H%M%SZ')
        hashed_payload = hashlib.sha256(body.encode('utf-8') if body else b"").hexdigest()
        canonical_headers = f"content-type:application/json\nhost:{host}\nx-sdk-date:{sdk_date}\n"
        signed_headers = "content-type;host;x-sdk-date"
        canonical_request = f"{method}\n{uri}\n{canonical_qs}\n{canonical_headers}\n{signed_headers}\n{hashed_payload}"
        hashed_req = hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()
        string_to_sign = f"SDK-HMAC-SHA256\n{sdk_date}\n{hashed_req}"
        signature = hmac.new(sk.encode('utf-8'), string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()
        h = js.Headers.new()
        h.append("Content-Type", "application/json")
        h.append("X-Sdk-Date", sdk_date)
        h.append("Host", host)
        h.append("Authorization", f"SDK-HMAC-SHA256 Access={ak}, SignedHeaders={signed_headers}, Signature={signature}")
        return h