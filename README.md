# YouTube Data Harvesting and Warehousing with Streamlit

## Overview
This Streamlit application is designed to fetch data from YouTube using the YouTube Data API, store it in a MongoDB database, and perform various queries on a MySQL database. It provides functionalities to retrieve channel details, video details, and comments, as well as execute predefined SQL queries to analyze the data.

## Features
Fetch YouTube channel details, including name, description, subscriber count, and video information.
Store retrieved data in a MongoDB database for efficient data storage and retrieval.
Perform predefined SQL queries on a MySQL database to analyze the stored YouTube data.
Visualize query results and display them in tabular format using the Streamlit framework.

## Installation
To run this application locally, follow these steps:

1. Install the required Python libraries by running
2. Configure your MongoDB Atlas connection string in the code.
3. Ensure you have a MySQL database set up and configured with the necessary tables.
4. Replace the MySQL connection details in the code with your database credentials.
5. Obtain a YouTube Data API key from the Google Cloud Console and replace the api_key variable in the code.
6.Run the Streamlit application by executing: streamlit run app.py

## Usage
1. Upon running the application, a Streamlit web interface will be launched in your default web browser.
2. Use the sidebar to input a YouTube video ID and fetch channel details.
3. View tables containing channel information, video details, and comments by clicking on the respective buttons in the sidebar.
4. Execute predefined SQL queries by selecting an option from the "Query View" dropdown menu.

## Dependencies
Streamlit: 0.88.0
Pandas: 1.3.3
pymongo: 3.12.0
mysql-connector-python: 8.0.27
google-api-python-client: 2.31.0# youtubedata
