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

const puppeteer = require('puppeteer');

// Process arguments
const args = require('yargs').parse();

(async () => {
    // Launch the browser and setup a 1024x2048 page
    const browser = await puppeteer.launch();
    const page = await browser.newPage();
    await page.setViewport({
        width: parseInt(args.width) || 1024,
        height: parseInt(args.height) || 2048,
    });

    // Wait until the page has fully loaded
    await page.goto(args.url, { waitUntil: 'networkidle2' });

    // Find the first button which has the word "death" in it
    const { nodes } = await page._client.send('Accessibility.getFullAXTree');
    // console.log(JSON.stringify(nodes, null, 2));
    const elem = nodes.filter(elem =>
        elem.name.value.toLowerCase() === args['button-text'])[0];
    // console.log(JSON.stringify(elem, null, 2));

    // Get the X,Y coordinates of the button
    const nodeBox = await page._client.send('DOM.getBoxModel', {
        backendNodeId: parseInt(elem.backendDOMNodeId)
    });
    const centerX = (nodeBox.model.content[0] + nodeBox.model.content[2]) / 2;
    const centerY = (nodeBox.model.content[1] + nodeBox.model.content[3]) / 2;

    // Click on the deaths tab and wait for the report to load
    await page.mouse.click(centerX, centerY);
    // await new Promise((resolve, _) => setTimeout(() => { resolve() }, 5000))
    try {
        await page.waitForNavigation({ timeout: 5000 });
    } catch (exc) {
        // No-op
    }

    // Get the contents of the whole page and write them to a file
    await page.screenshot({ path: args.output });
    await browser.close();
})();
