# Setup Instructions for connecting MongoDB, the C# driver, and GCP
This guide will run you through setting up MongoDB on gcloud, and connect to the database instance remotely.

<hr/>

## Resources
<i>
[MongoDB on Compute Engine](https://console.cloud.google.com/marketplace/details/click-to-deploy-images/mongodb)
[MongoDB Compute Engine Tutorial](https://blog.codecentric.de/en/2018/03/cloud-launcher-mongodb-google-compute-engine/)
[C# MongoDB Examples](https://www.codementor.io/pmbanugo/working-with-mongodb-in-net-1-basics-g4frivcvz)
</i>

<hr/>

## GCP setup
From the first link, 'MongoDB on Compute Engine'...
1. Click 'Launch on Compute Engine'
  - It should bring you to a page with 'New MongoDB deployment'
1. Zone: us-west1-x

*Servers Tier*
1. Instances Count: 2
1. Machine type: small (1 shared vCPU)
1. Data disk size in GB (20)

*Arbiters Tier*
1. Instances Count: 1
1. Machine type: small (1 shared vCPU)`

*Wait for it to deploy...*
(You should get an e-mail when it is complete)

1. SSH connect to any node and run `mongo`; `rs.status()` to check the replica staus
1. Power off all machines when you are done


## Manage the GCP firewall for external access to the db
1. In GCP, expand the hanburger menu
1. Under Networking, VPC network > Firewall rules
1. Create firewall rule
1. Enter some name, like allow-psu
1. Direction of traffic > ingress
1. Targets > All instances in the network
1. Source filter > IP range
1. For PSU, enter 131.252.0.0/16 _(16 is the CIDR subnet mask for 255.255.0.0, 24 is for 255.255.255.0)_
1. Else, go to *https://who.is/* to see your public IP
1. Protocols and ports > Specified protocols and ports
1. In the box, enter: tcp:27017
1. Save


## C# Driver setup via Visual Studios
1. Download Visual Studios
1. Create a new Project; Console App (.NET Core)
1. Once created, go to Project > Manage NuGet packages
1. Install MongoDB.Driver and MongoDB.Bson
1. Install CsvHelper
1. Copy the code from Program.cs
1. Change the MongoClient connection string to use your primary node's external IP
- Must have a majority of the instances running, so that they can hold an election & elect a primary
- I just run one data-bearing instance and the arbiter node
- External IP is on Compute Engine > VM Instances
- You can verify if the instance is primary or secondary via gcloud ssh
1. Be sure to copy the listings.csv file to the project folder
- I created a 'data' folder in the project root dir
- The program executes from rootdir/bin/Debug/netcoreapp3.0
- At the very least, be sure to change 'src' var to match the .csv file for listings
1. Run
