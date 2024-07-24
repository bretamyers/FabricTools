# Microsoft Fabric - Get Notebook Defnition Example

This notebook contains example code for getting the defintion of a Microsoft Fabric notebook from within a workspace. 
The first cell contains wrapper code around the Fabric APIs to abstract the logic and handle long running and paginated API calls.
The second cell contains sample code on how to use fabric_rest object to extract a notebook definition. The result is printed out to the user.

<br>

#### How to Run
- Run the first cell to load the fabric_rest() object.
- Update and run the workspace name and notebook name in the second cell to the workspace and notebook that you have permissions to within a Fabric workspace.
