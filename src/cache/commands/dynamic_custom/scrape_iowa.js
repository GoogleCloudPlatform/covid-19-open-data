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
    const browser = await puppeteer.launch({ headless: true, args: ['--no-sandbox'] });
    const page = await browser.newPage();
    await page.goto(args.url).catch(exc => void exc);
    const response = await page.waitForResponse(response => {
        return response.url().indexOf('/cards/1308116147/render') !== -1;
    }, { timeout: 10000 });
    try {
        await fs.writeFile(args.output, JSON.stringify((await response.json())));
    } catch (exc) {
        console.error(`Error parsing response: ${exc}`);
    }
    await browser.close();
})();
