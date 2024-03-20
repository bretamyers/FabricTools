# Microsoft Fabric Capacity Autoscale

A solution to automate the scaling of a capacity based on the consumption of the capacity units to stay within the bounds of a capacity and to be cost effective as possible. A Fabric capacity has a concept of bursting and smoothing capacity units over a period of time. This works great when the workload is consistent day to day but does not handle for scenarios for unexpected or inconsitent loads. 

<br>

**Target Scenarios**
- Development environments typically don’t have a consistent workload day to day.
- PoC/MVP where you to a day zero load of data with high usage and have gaps in days when the solution is worked on.
- Production environment when there is inconsistent user activity day to day where users run different load sizes.
- Production workload where there are abnormal workloads throughout the month. Example, month end activities require larger loads for data reconciliation.

<br>

## Prerequisites
- An Azure service principal
- A Fabric capacity created within the Azure portal and assign the service principal as a capacity admin.
- Enable the "Allow service principals to create and use profiles" setting in the admin portal of Microsoft Fabric.
- Deployment of the Fabric Capacity Metrics App as a workspace.
- Grant admin access to the service principal for the workspace that the Fabric Capacity Metircs App is deployed within.

<br>

## How to Deploy
- Log into **[app.fabric.microsoft.com](https://app.fabric.microsoft.com/home?experience=data-engineering)** and navigate to the Data Engineering persona.
- Within the Data Engineering persona landing page, click on the "Import notebook" icon.
- Upload the **NB_CapacityAutoScale.ipynb** file. This will upload the notebook to the Fabric workspace.
- Update the parameters within the NB_CapacityAutoScale notebook.
- Schedule the notebook to run by clicking on the **Run** tab at the top and click on the **Schedule** button. Turn on the scheduled run and add a frequency for how often you want to the notebook to run. 

<br>

## Notebook Parameters
- **minSku** - This is the smallest SKU that we'll scale to. This becomes important for features that are only available at a higher SKU. For example, we need to be at least an F64 to be able to use private endpoints.
- **maxSku** - This is the largest SKU that we'll scale to. This helps in keeping the cost within a defined budget.
- **utilizationTolerance** - A capacity utilization percentage that we want to stay under.
- **capacityName** - The capacity name that we want to automate the scaling operations.
- **subscriptionId** - The Azure subscription id that the Fabric capacity is created within.
- **metricsAppWorkspaceName** - The workspace that the Fabric Capacity Metrics App is deployed.
- **metricsAppModelName** - The semantic model that the Fabric Capacity Metrics App uses. The default name is "Fabric Capacity Metrics".

