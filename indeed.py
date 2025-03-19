import asyncio
import datetime
from threading import Lock
import pandas as pd
from  patchright.async_api import async_playwright,Page





PATTERNS = [
    "https://za.indeed.com/pagead/clk?",
    "https://za.indeed.com/rc/clk?"
]



listings = set()
listingsLock = Lock()
clicked_links = set()


async def scrapeListing(page: Page, now):
    def clean_text(text):
        lines = [
            line.strip().replace('\xa0', ' ') 
            for line in text.split('\n') 
            if line.strip()
        ]
        return ' '.join(lines)

    # Extract job title.
    
    try:
        title = await page.locator(".jobsearch-JobInfoHeader-title-container").text_content(timeout=500)
    except Exception:
        try:
            title = await page.locator("[data-testid='simpler-jobTitle']").text_content(timeout=500)
        except Exception:
            try:
                title = await page.locator("[data-testid='jobsearch-JobInfoHeader-title']").text_content(timeout=500)
            except Exception:
                title ="None"
                

    # Extract company name.
    try:
        company = await page.locator("[data-testid='inlineHeader-companyName'] a").text_content(timeout=500)
    except Exception:
        try:
            company = await page.locator("#jobsearch-ViewjobPaneWrapper > div.fastviewjob.jobsearch-ViewJobLayout--embedded.css-1sis433.eu4oa1w0.hydrated > div.jobsearch-JobComponent.css-1kw92ky.eu4oa1w0 > div.jobsearch-HeaderContainer.css-1obbpc8.eu4oa1w0 > div > div.css-1i8duct.e37uo190 > div > div").text_content(timeout=500)
            company = company.split("&")[0]
        except Exception:
            company = "None"
    
     
    # Extract location.
    try:
        location = await page.locator("[data-testid='inlineHeader-companyLocation'] div").text_content(timeout=500)
    except Exception:
        # Fallback selector for cases like the third example.
        try:
            location = await page.locator("#jobLocationText [data-testid='jobsearch-JobInfoHeader-companyLocation'] span").text_content(timeout=500)
        except Exception:
            location = "None"

    # Extract job description.
    try:
        description = await page.locator("#jobDescriptionText").inner_text(timeout=500)
    except Exception:
        description = "None"

    # Extract job type or salary info.
    try:
        # Try to get the job type element 
        job_type = await page.locator("#salaryInfoAndJobType .css-1h7a62l").text_content(timeout=500)
    except Exception:
        # If not available, try to extract the salary info instead.
        try:
            job_type = await page.locator("#salaryInfoAndJobType .css-1jh4tn2").text_content(timeout=500)
        except Exception:
            job_type = "None"

    # Clean the description text.
    cleaned_description = clean_text(description)

    listing = {
        "title": title.strip() if title else "None",
        "company": company.strip() if company else "Recruitment/None",
        "location": location.strip() if location else "Remote/None",
        "description": cleaned_description,
        "job_type": job_type.strip() if job_type else "None",
        "url": page.url
    }
    

    # Add listing if it hasn't been seen before.
    with listingsLock:
        if frozenset(listing.items()) not in listings:
            listings.add(frozenset(listing.items()))
            print("-" * 100)
            print(f"title: {listing['title']}")
            print(f"Company: {listing['company']}")
            print(f"location: {listing['location']}")
            print(f"type: {listing['job_type']}")
            #print(f"description:\n{listing['description']}")
            print("Elapsed time:", datetime.datetime.now() - now)
            print("Total jobs scraped:", len(listings))
    
async def scroll_and_scrape(page: Page, x=329, y=420, step=200, delay=0.1, now=None):
    """
    Slowly scroll down the page while querying multiple mouse positions for a valid link.
    At each scroll step, the function checks several positions on the screen.
    If a link is found that starts with one of the specified PATTERNS and hasn't been clicked yet, it:
      1. Performs CloudFlare bypass if necessary.
      2. Clicks the link at its specific coordinates.
      3. Waits for the page to load.
      4. Runs anti-scraping logic (navigates back if the URL starts with "https://za.indeed.com/viewjob?").
      5. Scrapes job details from the page.
      6. Returns to the original scroll position.
    
    :param page: The Playwright page object.
    :param x: Base x-coordinate (default 329).
    :param y: Base y-coordinate (default 420).
    :param step: Pixels to scroll down each step.
    :param delay: Delay (in seconds) between scroll steps.
    :param now: (Optional) A datetime value for logging elapsed time.
    :return: A dictionary with scraped job details or None if no new link is found.
    """
    # Define a list of positions to query around the base coordinate.
    positions = [(x + dx, y + dy) for dx in range(-50, 51, 10) for dy in range(-50, 51, 10)][:100]

    while True:
        # Get current scroll information.
        scrollY = await page.evaluate("() => window.scrollY")
        innerHeight = await page.evaluate("() => window.innerHeight")
        scrollHeight = await page.evaluate("() => document.body.scrollHeight")
        
        # Stop if near the bottom.
        if scrollY + innerHeight >= scrollHeight:
            print("Reached bottom of the page without finding a new valid link.")
            break
        
        # Query multiple positions for hovered links.
        hovered_links = await page.evaluate('''(positions) => {
            return positions.map(([x, y]) => {
                const el = document.elementFromPoint(x, y);
                const url = el ? el.closest('a')?.href : null;
                return {url: url, x: x, y: y};
            });
        }''', positions)
        
        # Iterate over each returned link and process if it meets the criteria.
        found_link = False
        for link_info in hovered_links:
            url = link_info.get('url')
            if url and any(url.startswith(pattern) for pattern in PATTERNS):
                if url in clicked_links:
                    continue
                # Valid new link found.
                found_link = True
                await page.mouse.click(link_info['x'], link_info['y'])
                clicked_links.add(url)
                await asyncio.sleep(0.5)
                
                # CloudFlare bypass logic.
                cloudFlareNavigation = await page.locator(".error").is_visible()
                while cloudFlareNavigation:
                    print(f"on scroll - CloudFlare navigation active: {cloudFlareNavigation}")
                    await CloudFlareBypass(page=page)
                    await asyncio.sleep(2)
                    cloudFlareNavigation = await page.locator(".error").is_visible()
                    if not cloudFlareNavigation:
                        break
                
                # Anti-scraping: if the page URL starts with the viewjob pattern, navigate back.
                while page.url.startswith("https://za.indeed.com/viewjob?"):
                    await page.go_back()
                    await asyncio.sleep(1)
                    break
                
                await scrapeListing(page=page, now=now)
                break  # Process only one new link per scroll step
        
        # Scroll down by the specified step and reposition the mouse to the base position.
        await page.evaluate(f"() => window.scrollBy(0, {step})")
        await page.mouse.move(x, y)
        await asyncio.sleep(delay)
    
    return None
  
async def scroll(page,x,y):
    await page.evaluate(f"window.scrollTo({x},{y})")

async def CloudFlareBypass(page:Page):
    await asyncio.sleep(3)
    button = await page.locator("xpath=//*[@id and string-length(@id)=5]").first.is_visible()
    
    if button:
        await page.locator("xpath=//*[@id and string-length(@id)=5]").first.click(force=True)
   
    await asyncio.sleep(1)
    
async def get_hovered_url(page, x, y):
    """
    Get the URL of the closest <a> element at the given mouse coordinates
    if it matches the specified patterns.
    """
    await page.wait_for_timeout(500)  # Wait for any hover effects to take place

    url = await page.evaluate("""
        ([x, y]) => {
            let elem = document.elementFromPoint(x, y);
            while (elem && elem.tagName !== 'A') {
                elem = elem.parentElement;  // Traverse up the DOM tree
            }
            return elem ? elem.href : null;  // Return the URL if an <a> element is found
        }
    """, [x, y])

    if url and any(url.startswith(pattern) for pattern in PATTERNS):
        return url
    return None

async def instance(url,start_index,now):

    def modify_url(url, new_start):
        base_url = url.split('&start=')[0]
        new_url = f"{base_url}&start={new_start}"
        return new_url
    
    async with async_playwright() as p:
        browser =  await p.chromium.launch(
                             
            headless=False,
            
        )
        context = await browser.new_context(viewport={"width": 1280,"height": 720})
        page = await context.new_page()
        page.set_default_timeout(31536000)
        await page.goto(url)
        await asyncio.sleep(3)

        cloudFlareOnEntry = await page.locator(".error").is_visible()
        print(f"on entry - {cloudFlareOnEntry}")
        while cloudFlareOnEntry :
            await CloudFlareBypass(page)
            cloudFlareOnEntry = await page.locator(".error").is_visible()

        await asyncio.sleep(2)  
    
        
        for i in range(1,60):
            for ii in range(1,13):
                await scroll_and_scrape(page=page,now=now)
                print(ii)
            await page.goto(modify_url(page.url,start_index+(10*i)))


            
            
       
        await asyncio.sleep(20)


async def main():
    
    now = datetime.datetime.now()
    
    #I get better results when scraping using 3 browser instances using one machine
    #Currently i am using 70 pages per scraper, 70*3*15(job posts per scraper) = 3150 job postings
    urls = [ 
            

            ("https://za.indeed.com/jobs?q=&l=South+Africa&radius=100&start=0",0,now),
            ("https://za.indeed.com/jobs?q=&l=South+Africa&radius=100&start=700",700,now),
            ("https://za.indeed.com/jobs?q=&l=South+Africa&radius=100&start=1400",1400,now),
            ]
        
    tasks = [asyncio.create_task(instance(*arg)) for arg in urls]
    await asyncio.gather(*tasks)
        
    
asyncio.run(main())
