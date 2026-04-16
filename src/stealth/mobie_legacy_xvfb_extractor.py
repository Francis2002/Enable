import time
import os
import random
import logging
from datetime import datetime, timezone
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By

# Setup paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))
LABELS_DIR = os.path.join(BASE_DIR, 'data', 'raw', 'mobie_labels')
os.makedirs(LABELS_DIR, exist_ok=True)

# Setup logging
log_file = os.path.join(LABELS_DIR, 'extraction.log')
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def run():
    # 1. Jitter: Sleep randomly between 0 and 60 seconds
    jitter = random.randint(0, 60)
    logging.info(f"--- Started scheduled run. Sleeping for {jitter} seconds (Jitter) ---")
    time.sleep(jitter)
    
    logging.info("Initializing browser...")
    options = uc.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    # Narrow portrait resolution to bypass width bounds limits
    options.add_argument('--window-size=800,600')
    options.add_argument('--disable-blink-features=AutomationControlled')
    
    prefs = {
        "download.default_directory": LABELS_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)

    driver = None
    try:
        driver = uc.Chrome(options=options, version_main=146)
        # 60s timeout for initial page load
        driver.set_page_load_timeout(60)
        
        logging.info("1. Navigating to Mobi.E map...")
        driver.get("https://www.mobie.pt/pt/carregarveiculo/encontrar-posto")
        time.sleep(15)
        
        logging.info("2. Clicking 'Postos' tab...")
        tabs = driver.find_elements(By.CSS_SELECTOR, ".find-station-tab")
        for tab in tabs:
            if "postos" in tab.text.lower() or "stations" in tab.text.lower():
                driver.execute_script("arguments[0].click();", tab)
                break
        time.sleep(5)
        
        logging.info("3. Clicking 'Limpar filtros'...")
        limpar = driver.find_elements(By.XPATH, "//*[contains(text(), 'Clear filters') or contains(text(), 'Limpar filtros')]")
        if limpar:
            driver.execute_script("arguments[0].click();", limpar[0])
            time.sleep(5)
            
        logging.info("3.5. Zooming out 2 times...")
        zoom_out = driver.find_elements(By.CSS_SELECTOR, ".leaflet-control-zoom-out")
        if zoom_out:
            for _ in range(2):
                zoom_out[0].click()
                time.sleep(2)
        
        logging.info("Waiting 10s for map bounds to completely stabilize...")
        time.sleep(10)
        
        logging.info("4. Clicking 'Filtrar'...")
        filter_btn = driver.find_elements(By.XPATH, "//button[contains(text(), 'Filter') or contains(text(), 'Filtrar')]")
        if filter_btn:
            driver.execute_script("arguments[0].click();", filter_btn[0])
        
        logging.info("Waiting 20s for stations API request to finish...")
        time.sleep(20)
        
        try:
            results = driver.find_elements(By.CSS_SELECTOR, ".find-station-results")
            if results:
                logging.info(f"Status API text: {results[0].text}")
        except Exception as e:
            logging.warning("Could not read API result text.")
            
        logging.info("5. Looking for the Export button...")
        export_btns = driver.find_elements(By.XPATH, "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'exportar') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'export')]")
        
        success = False
        for btn in export_btns:
            if btn.is_displayed():
                href = btn.get_attribute("href")
                if href and href.startswith("blob:"):
                    logging.info("Found blob href. Executing direct JS fetch...")
                    driver.set_script_timeout(30)
                    csv_text = driver.execute_async_script("""
                        var uri = arguments[0];
                        var callback = arguments[1];
                        fetch(uri).then(r => r.text()).then(t => callback(t)).catch(e => callback("ERROR:" + e));
                    """, href)
                    
                    if csv_text and not csv_text.startswith("ERROR:"):
                        # Filename using strictly UTC with 'Z' suffix
                        timestamp_utc = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
                        filename = f"{timestamp_utc}.csv"
                        filepath = os.path.join(LABELS_DIR, filename)
                        
                        with open(filepath, 'w', encoding='utf-8') as f:
                            f.write(csv_text)
                            
                        size_kb = len(csv_text) / 1024
                        logging.info(f"SUCCESS! Saved {filename} ({size_kb:.2f} KB)")
                        
                        if size_kb < 100:
                            logging.warning(f"File {filename} is suspiciously small ({size_kb:.2f} KB). API Rate limit likely hit!")
                        
                        success = True
                    else:
                        logging.error(f"Blob fetch failed: {csv_text}")
                break
                
        if not success:
            logging.error("Failed to find or download via Export button.")
            
    except Exception as e:
        logging.error(f"Extraction completely failed with exception: {str(e)}")
    finally:
        if driver:
            driver.quit()
            logging.info("Browser successfully killed to free memory. Run complete.\n")

if __name__ == "__main__":
    run()