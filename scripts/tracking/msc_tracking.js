// Playwright script to fetch MSC tracking details
const { chromium } = require('playwright');

async function run(trackingId) {
    const browser = await chromium.launch({ headless: true });
    const page = await browser.newPage();
    try {
        await page.goto(`https://www.msc.com/track-a-shipment?trackingNumber=${trackingId}`);
        console.log(`Tracking MSC cargo: ${trackingId}`);
    } finally {
        await browser.close();
    }
}
