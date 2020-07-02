/**
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const fs = require('fs').promises;
const puppeteer = require('puppeteer');

// Process arguments
const args = require('yargs').parse();

(async () => {
    // Launch the browser and setup a 1024x2048 page
    const browser = await puppeteer.launch();
    const page = await browser.newPage();
    await page.setViewport({
        width: 1024,
        height: 2048,
    });

    // Wait until the page has fully loaded, then wait for an extra 5 seconds
    await page.goto(args.url, { waitUntil: 'networkidle2' });
    await new Promise((resolve, _) => setTimeout(() => { resolve() }, 5000))

    // If we were given additional instructions to execute on page, do it now
    if (args.eval) await page.evaluate(args.eval);

    // Get the contents of the whole page and write them to a file
    if (args.screenshot) {
        await page.screenshot({ path: args.output });
    } else if (args.accessibility) {
        const accessibility = await page.accessibility.snapshot();
        await fs.writeFile(args.output, JSON.stringify(accessibility, null, 2));
    } else {
        const contents = await page.content();
        await fs.writeFile(args.output, contents);
    }

    await browser.close();
})();
