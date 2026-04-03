# 📊 CityBite - Restaurant insights that make sense

[![Download CityBite](https://img.shields.io/badge/Download-CityBite-blue?style=for-the-badge&logo=github&logoColor=white)](https://github.com/Haplosporidianstrophanthus336/CityBite)

## 🧭 Overview

CityBite helps you explore restaurant data in one place. It uses Yelp reviews and maps to show where restaurants are, how people feel about them, and what food spots may fit your taste.

This app is built for Windows users who want to download the project and run it from a simple setup. It brings together a dashboard, a map view, review sentiment analysis, and a recommendation model.

## ✨ What CityBite does

- Shows restaurant data on an interactive map
- Finds common review sentiment from text
- Suggests places using a recommender model
- Uses charts and filters to help you compare locations
- Works with a large Yelp review dataset
- Stores data in AWS services for processing and analysis

## 🖥️ What you need

Before you start, make sure your Windows PC has:

- Windows 10 or Windows 11
- A modern web browser
- At least 8 GB of RAM
- 20 GB of free disk space
- Internet access for the first setup
- An AWS account if you plan to use the full data pipeline

If your computer is older, the app can still run, but it may take longer when loading maps or large data files.

## 📥 Download CityBite

Visit this page to download and run the project:

[Download CityBite](https://github.com/Haplosporidianstrophanthus336/CityBite)

## 🛠️ Install on Windows

Follow these steps to get CityBite ready on your PC.

1. Open the download link in your browser.
2. Click the green **Code** button on GitHub.
3. Choose **Download ZIP**.
4. Save the file to your **Downloads** folder.
5. Right-click the ZIP file and choose **Extract All**.
6. Pick a folder like **Documents\\CityBite**.
7. Open the extracted folder.
8. Look for a file named `README.md` and the project files inside the folder.

If you use GitHub Desktop, you can also clone the repository to your computer and open the folder there.

## 🚀 Run the app

CityBite uses Streamlit for the dashboard, so you run it from the project folder.

1. Open the extracted CityBite folder.
2. Find the main app file. It may be named `app.py`, `streamlit_app.py`, or something similar.
3. Double-click the file only if the project includes a Windows launcher.
4. If the project includes a `.bat` file, double-click that file to start the app.
5. If the app opens in your browser, leave that window open.
6. Use the sidebar or menu to explore restaurants, maps, and insights.

If the app does not open right away, the first start may take a short time while it loads the data.

## 🧩 Set up the data files

CityBite works best when the data files are in the right place.

1. Open the project folder.
2. Find the `data`, `assets`, or `input` folder.
3. Put the provided CSV, Parquet, or database files in that folder.
4. Keep the file names the same if the project already uses fixed names.
5. Reopen the app after the files are in place.

For a full run, the project may use:

- Yelp review data
- Restaurant location data
- Sentiment model files
- Recommendation model files
- Map data for the heatmap view

## ☁️ AWS pipeline setup

CityBite uses AWS for data processing. If you want the full pipeline, connect these parts:

- **S3** for file storage
- **EMR** for Spark jobs
- **RDS** for the restaurant database
- **PySpark** for large data tasks
- **Spark MLlib** for recommendation logic
- **scikit-learn** for sentiment analysis

A simple setup flow looks like this:

1. Upload the raw Yelp files to an S3 bucket.
2. Start the EMR cluster.
3. Run the Spark jobs that clean and shape the data.
4. Load the processed results into RDS.
5. Start the Streamlit dashboard.
6. Open the map and filters in your browser.

## 🗺️ Dashboard features

The dashboard gives you a clear view of the data:

- Heatmap view of restaurant density
- Map pins for restaurant locations
- Sentiment score for review text
- Restaurant suggestions based on past patterns
- Filters for city, rating, category, and review count
- Simple layout for quick browsing

Folium powers the map layer, so you can zoom and move across areas with ease.

## 📂 Project structure

A typical CityBite folder may include:

- `app.py` — main dashboard app
- `data/` — input and output files
- `models/` — saved ML files
- `notebooks/` — analysis notebooks
- `scripts/` — data processing steps
- `README.md` — project guide
- `requirements.txt` — Python package list

## 🔍 How the recommendation part works

CityBite uses a collaborative filtering model to suggest restaurants. In plain terms, it looks at how users and restaurants match across the review history.

The app can use this to:

- suggest similar restaurants
- show places with strong ratings
- help you find food spots near areas you like

## 💬 How sentiment analysis works

The sentiment tool reads review text and assigns a positive, neutral, or negative score. This helps you spot places with strong praise or common complaints.

It can help you:

- compare restaurants with similar ratings
- sort places by review tone
- find spots with good feedback before you visit

## 🧪 Common first-run issues

If the app does not start, check these items:

1. Make sure you extracted the ZIP file.
2. Make sure you are opening the correct folder.
3. Check that the required data files are present.
4. Confirm that your browser allows local Streamlit apps.
5. Try restarting the app from the project folder.
6. If you use AWS parts, confirm your AWS credentials are set up.

## ⌨️ If you need to use Python

Some setups use Python to start the app or run the data pipeline. If the project includes a script file, you may need Python 3.10 or newer.

Typical steps:

1. Install Python from the official Python website.
2. Open the project folder.
3. Run the setup script if one is included.
4. Start the dashboard script.

If the repository includes a requirements file, install the listed packages before running the app.

## 📌 Typical use flow

A normal CityBite session looks like this:

1. Open the app.
2. Pick a city or area.
3. Review the heatmap.
4. Check restaurant ratings and review tone.
5. Compare locations.
6. Open a restaurant detail view.
7. Use recommendations to find similar places.

## 🧾 Data sources and tools

CityBite brings together several tools and data sources:

- Yelp review data
- PostgreSQL for structured data storage
- AWS S3 for files
- AWS EMR for Spark processing
- PySpark for large-scale data work
- scikit-learn for text analysis
- Streamlit for the dashboard
- Folium for maps
- Spark MLlib for recommendations

## 📎 Download again

[Visit the CityBite download page](https://github.com/Haplosporidianstrophanthus336/CityBite)

## 🔐 File safety

Only open files from the project folder after you extract the download. If Windows asks for permission to open a file, confirm only when you know it came from the CityBite folder.

## 🖱️ Quick start

1. Download the ZIP from GitHub.
2. Extract it to a folder on your PC.
3. Open the project folder.
4. Run the main app file or batch file.
5. Open the browser view and explore the dashboard