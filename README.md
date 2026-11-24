# Ethiopian IT Job Finder (free, hostable version)

No server needed. A scheduled GitHub Action collects jobs from all the sites every
3 hours and publishes them, with the search page, on GitHub Pages. Search runs in the
visitor's browser, so results appear as you type, with pagination over thousands of jobs.

## Put it online (about 5 minutes)

1. Create a **public** repository on GitHub (public repos get free Actions minutes and Pages).
2. Upload everything in this folder to it, including the hidden `.github` folder. Use the branch `main`.
3. Go to **Settings > Pages** and set **Source** to **GitHub Actions**.
4. Open the **Actions** tab, choose **Update jobs and deploy**, press **Run workflow**.
5. When it finishes (a few minutes) your site is at `https://YOUR-NAME.github.io/REPO-NAME/`.

After that it refreshes by itself every 3 hours.

## Try it on your computer

    pip install requests
    python crawler.py --out site/data/jobs.json
    python -m http.server -d site 8000      # then open http://localhost:8000

## Customize

- **Sites:** edit the `SITES` list in `crawler.py`.
- **IT keywords:** edit `FIELDS` and `ROOTS` in `crawler.py`.
- **How old jobs can be:** change `MAX_AGE_DAYS` in `crawler.py` (default 90).
- **How often it updates:** change the `cron` line in `.github/workflows/update.yml`.

## Good to know

- GitHub pauses scheduled workflows after 60 days with no activity in the repository. If updates stop, open the Actions tab and re-enable it, or press Run workflow.
- Some sites may block GitHub's servers. The page lists sites that returned nothing.
- Facebook, LinkedIn, Indeed and Glassdoor can't be collected, so the page links to their own search.
