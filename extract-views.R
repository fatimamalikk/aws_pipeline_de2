## SUBJECT DATE
DATE_PARAM="2021-10-07"

date <- as.Date(DATE_PARAM, "%Y-%m-%d")

# install.packages('httr', 'jsonlite', 'lubridate')
library(httr)
library(aws.s3)
library(jsonlite)
library(lubridate)

url <- paste(
  "https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia.org/all-access/",
  format(date, "%Y/%m/%d"), sep='')


wiki.server.response = GET(url)
wiki.response.status = status_code(wiki.server.response)
print(paste('Wikipedia API Response: ', wiki.response.status, sep=''))

wiki.response.body = content(wiki.server.response, 'text')

if (wiki.response.status != 200){
  print(paste("Recieved non-OK status code from Wiki Server: ",
              wiki.response.status,
              '. Response body: ',
              wiki.response.body, sep=''
  ))
}

# Save Raw Response and upload to S3
RAW_LOCATION_BASE='data/raw-views'
dir.create(file.path(RAW_LOCATION_BASE), showWarnings = TRUE, recursive = TRUE)

########
# LAB  #
########
#
# Save `wiki.response.body` to the local filesystem into the folder defined 
# in variable `RAW_LOCATION_BASE` under the name `raw-views-YYYY-MM-DD.txt`,
# i.e: `data/raw-views/raw-views-2021-10-01.txt`.

#### ANSWER ####
raw.output.filename = paste("raw-views-", format(date, "%Y-%m-%d"), '.txt',
                            sep='')
raw.output.fullpath = paste(RAW_LOCATION_BASE, '/', 
                            raw.output.filename, sep='')
write(wiki.response.body, raw.output.fullpath)

########
# LAB  #
########
#
# Upload the file you created to S3.
#
# * Upload the file you created to your bucket (you can reuse your bucket from 
#   the previous classes or create a new bucket. Both solutions work.) 
# * Place the file on S3 into your bucket under the folder called `datalake/raw/`.
# * Don't change the file's name when you are uploading to S3, keep it at `raw-views-YYYY-MM-DD.txt`
# * Once you uploaded the file, verify that it's there (list the bucket in R, in the CLI or on the Web)


# BUCKET="{your bucket name}"
#
# {{ FILL IN AWS SETUP STEPS (you might need to copy your accessKey.csv to the working directory) }}
#

## Upload the file
# put_object(file = "{{ ADD LOCAL FILE PATH }}",
#            object = "{{ ADD FOLDER AND FILE NAME HERE in a form of FOLDER/FILE_NAME }}",
#            bucket = BUCKET,
#            verbose = TRUE)

#### ANSWER ####
keyfile = list.files(path=".", pattern="accessKeys.csv", full.names=TRUE)
if (identical(keyfile, character(0))){
  stop("ERROR: AWS key file not found")
} 

keyTable <- read.csv(keyfile, header = T) # *accessKeys.csv == the CSV downloaded from AWS containing your Access & Secret keys
AWS_ACCESS_KEY_ID <- as.character(keyTable$Access.key.ID)
AWS_SECRET_ACCESS_KEY <- as.character(keyTable$Secret.access.key)

#activate
Sys.setenv("AWS_ACCESS_KEY_ID" = AWS_ACCESS_KEY_ID,
           "AWS_SECRET_ACCESS_KEY" = AWS_SECRET_ACCESS_KEY,
           "AWS_DEFAULT_REGION" = "eu-west-1") 

BUCKET="fatima.arshad" # Change this to your own bucket

put_object(file = raw.output.fullpath,
           object = paste('datalake/raw/', 
                          raw.output.filename,
                          sep = ""),
           bucket = BUCKET,
           verbose = TRUE)


## Parse the response and write the parsed string to "Bronze"

# We are extracting the top views from the server's response
wiki.response.parsed = content(wiki.server.response, 'parsed')
top.views = wiki.response.parsed$items[[1]]$articles

# Convert the server's response to JSON lines
current.time = Sys.time() 
json.lines = ""
for (page in top.views){
  record = list(
    article = page$article,
    views = page$views,
    rank = page$rank,
    date = format(date, "%Y-%m-%d"),
    retrieved_at = current.time
  )
  
  json.lines = paste(json.lines,
                     toJSON(record,
                            auto_unbox=TRUE),
                     "\n",
                     sep='')
}

# Save the Top views JSON lines as a file and upload it to S3

JSON_LOCATION_BASE='data/views'
dir.create(file.path(JSON_LOCATION_BASE), showWarnings = TRUE)

json.lines.filename = paste("views-", format(date, "%Y-%m-%d"), '.json',
                            sep='')
json.lines.fullpath = paste(JSON_LOCATION_BASE, '/', 
                            json.lines.filename, sep='')

write(json.lines, file = json.lines.fullpath)

put_object(file = json.lines.fullpath,
           object = paste('datalake/views/', 
                          json.lines.filename,
                          sep = ""),
           bucket = BUCKET,
           verbose = TRUE)

