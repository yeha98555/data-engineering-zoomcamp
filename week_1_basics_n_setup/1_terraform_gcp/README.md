## Local Setup for Terraform and GCP

### Terraform

[reference](https://computingforgeeks.com/how-to-install-terraform-on-ubuntu/)
```shell
# install repository addition dependencies
sudo apt update sudo apt install software-properties-common gnupg2 curl

# import repository GPG key
curl https://apt.releases.hashicorp.com/gpg | gpg --dearmor > hashicorp.gpg
sudo install -o root -g root -m 644 hashicorp.gpg /etc/apt/trusted.gpg.d/

# With the key imported now add Hashicorp repository to your Ubuntu system
sudo apt-add-repository "deb [arch=$(dpkg --print-architecture)] https://apt.releases.hashicorp.com focal main"
# because apt repository has packages for Ubuntu 22.04 "jammy" yet, so here use "focal" (Ubuntu 20.04)

# check
terraform --version
```

### GCP

1. login [Google Cloud Platform](https://cloud.google.com/)
2. create a new project, and select it
    - Project name: dtc-de
3. go `IAM & Admin` on left menu, then go to `Service Accounts`
    - service account name: dtc-de-user
    - role: viewer (we'll set it later)
   then click `DONE` to finish creating service account.
4. click `...` -> `Manage keys` of the created service account
	1) `ADD KEY` -> `Create new key`
	2) choose `JSON`
5. install Google Cloud SDK  
[reference](https://cloud.google.com/sdk/docs/install#deb)
```shell
# install repository addition dependencies
sudo apt-get install apt-transport-https ca-certificates gnupg

# add the gcloud CLI distribution URI as a package source
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

# import the Google Cloud public key
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo tee /usr/share/keyrings/cloud.google.gpg

# update and install the gcloud CLI
sudo apt-get update && sudo apt-get install google-cloud-cli

# check
gcloud --version
gcloud -v
```
6. set environment variable to point to your download GCP auth-keys
```shell
export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"

# refresh token, and verify authentication
gcloud auth application-default login
# the broswer will appear to ask you to login and authenticate.
```

7. add access to `dtc-de-user` on Google Cloud Platform
   1) go to `IAM` on the left menu
   2) click `edit` icon of the `dtc-de-user`
   3) click `ADD ANOTHER ROLE`
   4) choose `Storage Admin`
   5) click `ADD ANOTHER ROLE`
   6) choose `Storage Object Admin`
   7) click `ADD ANOTHER ROLE`
   8) choose `BigQuery Admin`
   9) click `SAVE`
8. enable these APIs for the project
   - [Identity and Access Management (IAM) API](https://console.cloud.google.com/apis/library/iam.googleapis.com)
   - [IAM Service Account Credentials API](https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com)



