You can run the container with something like:
```
docker run -it -e AWS_ACCESS_KEY_ID=[ID] -e AWS_SECRET_ACCESS_KEY=[KEY] -e "DATAFILE=2011/08/24/KBGM/KBGM20110824_231327_V03.gz" -e "DEST_BUCKET=cu-cs-radar" vol2birds3 
```
