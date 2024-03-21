# Microsoft Fabric Capacity Autoscale

A solution to automate the scaling of a capacity based on the consumption of the capacity units to stay within the bounds of the capacity for cost optimization. A Fabric capacity has a concept of bursting and smoothing capacity units over a period of time. This works great when the workload is consistent day to day but does not handle for scenarios for unexpected or inconsitent loads. 

<br>

#### Target Scenarios
- Development environments typically don’t have a consistent workload day to day.
- PoC/MVP where you to a day zero load of data with high usage and have gaps in days when the solution is worked on.
- Production environments when there is unpredictable user activity day to day where users run different load sizes.
- Environments where there are inconsitent workloads throughout the month. Example, month end activities which may require larger data loads for data reconciliation and higher consumption of reports.
- Environments that have seasonal loads where theres a month or two throughout the year where within higher activity/comsumption. 

<br>

## Prerequisites
- An Azure service principal
- A Fabric capacity created within the Azure portal and assign the service principal as a capacity admin.
- Enable of **User can create Fabric items** within the admin port of Microsoft Fabric.
- Enable the **Allow service principals to create and use profiles** setting in the admin portal of Microsoft Fabric.
- Deployment of the Fabric Capacity Metrics App as a workspace.
- Grant admin access to the service principal for the workspace that the Fabric Capacity Metircs App is deployed within.

<br>

## How to Deploy
- Log into **[app.fabric.microsoft.com](https://app.fabric.microsoft.com/home?experience=data-engineering)** and navigate to the Data Engineering persona.
- Within the Data Engineering persona landing page, click on the **Import notebook** icon.
- Upload the **NB_CapacityAutoScale.ipynb** file. This will upload the notebook to the Fabric workspace.
- In first cell, update the parameters within the NB_CapacityAutoScale notebook.
- In the second cell, update the **tenantId**, **clientId**, and **secret** values to match those of the service principal. Note, the notebook includes code to acquire values from an Azure Key Vault if you want to keep the crendentials of the service principal secure.
- Schedule the notebook to run by clicking on the **Run** tab at the top and click on the **Schedule** button. Turn on the scheduled run and add a frequency for how often you want to the notebook to run. 
> Note: The defined deployment steps are for deploying the code as a notebook within a Fabric workspace but the code can be deployed and executed outside of a Fabric notebook. Example, running the code as a python runbook within Azure Automation.

<br>

## Notebook Parameters
- **minSku** - This is the smallest SKU that we'll scale to. This becomes important for features that are only available at a higher SKU. For example, we need to be at least an F64 to be able to use private endpoints.
- **maxSku** - This is the largest SKU that we'll scale to. This helps in keeping the cost within a defined budget.
- **utilizationTolerance** - A capacity utilization percentage that we want to stay under.
- **capacityName** - The capacity name that we want to automate the scaling operations.
- **subscriptionId** - The Azure subscription id that the Fabric capacity is created within.
- **metricsAppWorkspaceName** - The workspace that the Fabric Capacity Metrics App is deployed.
- **metricsAppModelName** - The semantic model that the Fabric Capacity Metrics App uses. The default name is "Fabric Capacity Metrics".

<br>

## Additional Resources
- **Operation Categorizations** - https://learn.microsoft.com/en-us/fabric/enterprise/fabric-operations#fabric-operations-by-experience

- **Throttling Rules** - https://learn.microsoft.com/en-us/fabric/enterprise/throttling#future-smoothed-consumption  

