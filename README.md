# Utils for Sonatype Nexus Repository Manager

Domain and credentials in `nexus-cleaner.conf`:
```
nexus_url=http_url
login=my_login
password=my_password
```

Usage:
```bash
# keep minimal required versions for each image
python nexus_cleaner.py

# keep last 20 images
python nexus_cleaner.py - c 20

# delete all older then 5 days
python nexus_cleaner.py -d 5

# delete all older then 5 days, also print info about all images
python nexus_cleaner.py -d 5 --full_info

# delete image with name `image1` older then 5 days
python nexus_cleaner.py -d 5 --names image1
```
