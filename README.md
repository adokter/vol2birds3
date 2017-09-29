# vol2birds3
This repository contains a containerized version of [vol2bird](https://github.com/adokter/vol2bird), with example scripts for running the container on Amazon AWS.

To generate a profile `profile.h5` for a local file `volume.h5` in directory `/my/local/directory`, run the container with:
```
docker run --rm -v /my/local/directory:/data adokter/vol2bird bash -c "cd data && vol2bird volume.h5 profile.h5"
```
