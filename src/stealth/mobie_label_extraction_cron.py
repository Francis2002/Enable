import time
import os
import random
import logging
from datetime import datetime, timezone
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains

# Setup paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))
LABELS_DIR = os.path.join(BASE_DIR, 'data', 'production', 'mobie_labels')
os.makedirs(LABELS_DIR, exist_ok=True)

# Setup logging - Only logging ERRORS per user request
log_file = os.path.join(LABELS_DIR, 'extraction.log')
logging.basicConfig(
    filename=log_file,
    level=logging.ERROR,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Setup XAUTHORITY if missing (required for background execution on dummy display)
if 'XAUTHORITY' not in os.environ:
    os.environ['XAUTHORITY'] = os.path.expanduser('~/.Xauthority')

def human_click(driver, element):
    """Perform a realistic click with ActionChains instead of JS"""
    action = ActionChains(driver)
    action.move_to_element(element)
    action.pause(random.uniform(0.1, 0.3))
    action.click()
    action.perform()

def run():
    # Jitter for the 5-minute cron
    jitter = random.randint(0, 30)
    time.sleep(jitter)

    if 'DISPLAY' not in os.environ:
        logging.error("DISPLAY not set. This script requires a graphical display.")
        return

    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu-sandbox')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.binary_location = "/usr/bin/google-chrome-stable"

    prefs = {
        "download.default_directory": LABELS_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)

    service = Service()
    driver = None
    try:
        driver = webdriver.Chrome(service=service, options=options)

        # Basic webdriver override to supplement the GPU fingerprint
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
            Object.defineProperty(navigator, 'webdriver', {
              get: () => undefined
            })
            """
        })

        driver.set_page_load_timeout(60)
        
        # 1. Navigate to Mobi.E map page
        driver.get("https://www.mobie.pt/pt/carregarveiculo/encontrar-posto")
        time.sleep(15)
        
        # 2. Click 'Postos' (Stations) tab
        tabs = driver.find_elements(By.CSS_SELECTOR, ".find-station-tab")
        for tab in tabs:
            if "postos" in tab.text.lower() or "stations" in tab.text.lower():
                try:
                    human_click(driver, tab)
                except:
                    driver.execute_script("arguments[0].click();", tab)
                break
                
        time.sleep(random.uniform(4.5, 5.5))
        
        # 3. Click 'Limpar filtros' (Clear Filters)
        limpar = driver.find_elements(By.XPATH, "//*[contains(text(), 'Clear filters') or contains(text(), 'Limpar filtros')]")
        if limpar:
            try:
                human_click(driver, limpar[0])
            except:
                driver.execute_script("arguments[0].click();", limpar[0])
            time.sleep(random.uniform(4.5, 5.5))

        # 3.5 Zooming out 8 times to capture the entire country on 1080p
        zoom_out = driver.find_elements(By.CSS_SELECTOR, ".leaflet-control-zoom-out")
        if zoom_out:
            for i in range(8):
                try:
                    human_click(driver, zoom_out[0])
                except:
                    driver.execute_script("arguments[0].click();", zoom_out[0])
                time.sleep(random.uniform(1.5, 2.5))
        
        time.sleep(10)

        # 4. Click 'Filtrar' (Filter) button to load ALL stations
        filter_btn = driver.find_elements(By.XPATH, "//button[contains(text(), 'Filter') or contains(text(), 'Filtrar')]")
        if filter_btn:
            try:
                human_click(driver, filter_btn[0])
            except:
                driver.execute_script("arguments[0].click();", filter_btn[0])
        
        time.sleep(20)
        
        try:
            results = driver.find_elements(By.CSS_SELECTOR, ".find-station-results")
            if results:
                # Optionally check if 0 postos were found, if so log it as an error
                if "0 postos" in results[0].text:
                    logging.error(f"Bot detection triggered or API empty: {results[0].text}")
        except Exception:
            logging.error("Could not read API result text.")
        
        # 5. Look for the Export button
        export_btns = driver.find_elements(By.XPATH, "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'exportar') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'export')]")
        
        success = False
        for btn in export_btns:
            if btn.is_displayed():
                href = btn.get_attribute("href")
                if href and href.startswith("blob:"):
                    driver.set_script_timeout(30)
                    csv_text = driver.execute_async_script("""
                        var uri = arguments[0];
                        var callback = arguments[1];
                        fetch(uri).then(r => r.text()).then(t => callback(t)).catch(e => callback("ERROR:" + e));
                    """, href)
                    
                    if csv_text and not csv_text.startswith("ERROR:"):
                        timestamp_utc = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
                        filename = f"{timestamp_utc}.csv"
                        csv_path = os.path.join(LABELS_DIR, filename)
                        
                        with open(csv_path, 'w', encoding='utf-8') as f:
                            f.write(csv_text)
                            
                        size_kb = len(csv_text) / 1024
                        
                        if size_kb < 100:
                            logging.error(f"Downloaded file is suspiciously small ({size_kb:.2f} KB). Possible API limit hit.")
                        
                        success = True
                    else:
                        logging.error(f"Blob fetch failed: {csv_text}")
                else:
                    try:
                        human_click(driver, btn)
                    except:
                        driver.execute_script("arguments[0].click();", btn)
                    success = True
                break
                
        if not success:
            logging.error("Failed to find or download via Export button.")

    except Exception as e:
        logging.error(f"Extraction completely failed with exception: {str(e)}")
    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    run()