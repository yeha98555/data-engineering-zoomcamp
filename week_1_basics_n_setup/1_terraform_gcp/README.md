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

VSCode Extensions
- HashiCorp Terraform

### GCP

#### Create Service Account and Gnerate Key

1. login [Google Cloud Platform](https://cloud.google.com/)
2. create a new project, and select it
    - Project name: dtc-de
3. go `IAM & Admin` on left menu, then go to `Service Accounts`
    - service account name: dtc-de-user
    - role: viewer (we'll set it later)<br>
   
   then click `DONE` to finish creating service account.

4. click `...` -> `Manage keys` of the created service account
	1) `ADD KEY` -> `Create new key`
	2) choose `JSON` 

5. add access to `dtc-de-user` on Google Cloud Platform
   1) go to `IAM` on the left menu
   2) click `edit` icon of the `dtc-de-user`
   3) click `ADD ANOTHER ROLE`
   4) choose `Storage Admin` (Cloud Storage)
   5) click `ADD ANOTHER ROLE`
   6) choose `BigQuery Admin` (BigQuery)
   7) click `ADD ANOTHER ROLE`
   8) choose `Compute Admin` (Compute Engine)
   9) click `SAVE`

#### Use key (Google Credentials)

##### Terraform
set Google Credentials for each project.

1. add `credentials` line to `provider` in `main.tf`
```tf
provider "google" {
  project     = var.project
  region      = var.region
  credentials = file(var.credentials) # Use this if you do not want to set env-var GOOGLE_APPLICATION_CREDENTIALS
}
```

2. add below three liens to `variables.tf`
```tf
variable "credentials" {
  description = "Path to your GCP Service Account JSON file"
  type        = string
}
```

##### Google Cloud SDK
set Google Credentials as default for PC.

1. install Google Cloud SDK  
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
2. set environment variable to point to your download GCP auth-keys
```shell
export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"
# if want to cancel, unset GOOGLE_APPLICATION_CREDENTIALS

# refresh token, and verify authentication
gcloud auth application-default login
# the broswer will appear to ask you to login and authenticate.
```

3. enable these APIs for the project
   - [Identity and Access Management (IAM) API](https://console.cloud.google.com/apis/library/iam.googleapis.com)
   - [IAM Service Account Credentials API](https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com)



