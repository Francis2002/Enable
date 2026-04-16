# Mobi.E Extraction - Stealth Mode Strategy

## 1. Problem Statement & Failed Approaches
**The Issue:** The scheduled extractor (`mobie_scheduled_extractor.py`) successfully loads the page and interacts with the UI but receives `0 postos e 0 tomadas` from the API upon clicking "Filtrar". 
**The Discovery:** Performing the exact same steps manually on the same server/IP succeeds without issues.
**Conclusion:** Mobi.E is utilizing an Advanced Web Application Firewall (WAF) / Bot Detector that is successfully distinguishing our Selenium script from a human based on hardware/environment fingerprinting upon page load. It is **not** an IP ban.

**Why the previous approaches failed:**
1.  **Hardware Fingerprinting (The Smoking Gun):** We ran a fingerprinting test comparing a real session with the dummy monitor (`Antibot_w_monitor.pdf`) against our `xvfb` headless session (`xvfb_fingerprint.json`). 
    - *Real Monitor (PDF):* Successfully reports WebGL Vendor: `NVIDIA Corporation` and Renderer: `NVIDIA GeForce GTX 980, or similar`.
    - *xvfb Bot Session:* Reports **WebGL not supported**. Advanced bot detectors immediately flag browsers missing WebGL contexts as headless scrapers.
2.  **Robotic Interactions:** We used synthetic JavaScript `.click()` commands. Bot detectors monitor mouse trajectories and trigger events. A `.click()` without preceding `mousemove` and `mousedown` events is an instant red flag.
3.  **Headless Signatures:** Running inside `xvfb` causes the browser to default to `24-bit` color depth, `1.0` Device Pixel Ratio, and default Linux User-Agents, all of which contrast with the typical signatures of a modern consumer desktop.

## 2. The Upgrades (Stealth Implementation)

### A. Hardware Acceleration Bypass (Ditch xvfb)
Since the `xvfb` emulator causes WebGL to crash or report as unsupported, we will abandon `xvfb`. We will run the Chrome driver directly onto the existing Dummy HDMI display (e.g., `DISPLAY=:0` or `:1`) to inherit the real NVIDIA GPU acceleration, bypassing the WebGL bot checks entirely.

### B. Realistic Clicks (ActionChains)
We will abandon all synthetic JavaScript `.click()` commands. We will implement Selenium `ActionChains` to:
*   Move the virtual mouse cursor physically over the element.
*   Simulate a genuine `mousedown` event, followed by a micro-pause, and a `mouseup` event.

### C. Micro-Jitter Timings
All rigid `time.sleep()` calls will be replaced with randomized boundaries. 
*   *Old:* `time.sleep(5)`
*   *New:* `time.sleep(random.uniform(4.2, 6.8))`

### D. Cursor Trajectories & Synthetic Scrolling
Instead of the mouse teleporting instantly, we will generate intermediate "waypoints". The cursor will sweep across the screen, mimicking a human hand. We will also inject small scrolling events.

## 3. Implementation Phase (The Test Script)
We have created `src/stealth/get_xvfb_fingerprint.py` to diagnose the `xvfb` leakage.
Next, we will create `src/stealth/mobie_stealth_test.py` to attempt the extraction using `DISPLAY=:0` (hardware passthrough) and ActionChains.

## 4. Results & Comments
*   **Run 1 (Fingerprint Audit):** Confirmed `xvfb` fails WebGL checks. 
*   **Next Steps:** User to verify running the script with `DISPLAY=:0` via the Dummy HDMI plug to confirm hardware acceleration passthrough.

## 5. Discoveries and Fixes for Dummy HDMI & Headless Execution

During the testing phase to transition from `xvfb` to `DISPLAY=:0` (Dummy HDMI), we encountered several OS-level and browser-level blockers that prevented Chrome from launching. Below is the documentation of these issues and their permanent fixes:

### A. The `XAUTHORITY` Requirement
**Issue:** When executing a script via SSH or a background cron job targeting a physical display (`DISPLAY=:0`), the X11 server rejects the connection with "Invalid MIT-MAGIC-COOKIE-1 key" or "Missing X server". 
**Fix:** The script must authenticate with the graphical session. We fixed this by explicitly passing the `XAUTHORITY` environment variable, pointing to the user's magic cookie file:
`XAUTHORITY=/home/joao-martins/.Xauthority`

### B. LightDM Auto-Login (The "Locked Screen" Blocker)
**Issue:** If the server reboots and sits at the Ubuntu login screen (LightDM), the `joao-martins` user does not have an active graphical session. The OS blocks `DISPLAY=:0` access, causing `SessionNotCreatedException` (Chrome not reachable).
**Fix:** We configured LightDM to automatically log in the `joao-martins` user on boot.
*   **Implementation:** Created `/etc/lightdm/lightdm.conf.d/50-my-autologin.conf` with:
    ```ini
    [Seat:*]
    autologin-user=joao-martins
    ```

### C. Snap Chromium vs. undetected_chromedriver
**Issue:** Ubuntu's default `chromium-browser` is a Snap package. Snap's strict AppArmor sandboxing prevents `undetected_chromedriver` from patching the binary and blocks it from binding to local debugging ports, resulting in silent crashes (`session not created`).
**Fix:** We abandoned the Snap version and installed the official `.deb` release of Google Chrome (`google-chrome-stable`). We updated the script to explicitly use this binary via `browser_executable_path="/usr/bin/google-chrome-stable"`.

### D. Profile Isolation (Concurrency Collisions)
**Issue:** Running the manual test script (`test_dummy_extraction.py`) simultaneously with the scheduled cron job (`mobie_scheduled_extractor.py`) caused them to fight over the same default Chrome profile lock, crashing both instances.
**Fix:** We isolated the test environment by explicitly defining a separate profile directory:
`options.add_argument('--user-data-dir=/home/joao-martins/dummy_chrome_profile_test')`