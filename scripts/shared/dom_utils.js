// Common DOM selection and interaction helpers for Playwright scripts
async function waitForElement(page, selector, timeout = 10000) {
    await page.waitForSelector(selector, { timeout });
}

async function getText(page, selector) {
    return await page.locator(selector).first().innerText();
}

module.exports = {
    waitForElement,
    getText
};
