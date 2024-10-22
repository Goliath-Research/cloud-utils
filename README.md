# Introduction 
As we will start processing the data with our current resources (laptops, Titan servers, and cloud virtual machines),
we will not have in place Kubernetes nor orchestrated workflows flexible enough for our needs.
The first temporary solution will require manual processing with Python scripts to interact with several kinds of storage:
1. FTP
2. S/FTP
3. File System (local and network shares)
4. Amazon S3
5. Azure Blob

# Architecture
The plan is to create an abstract class Storage that should expose methods for:
1.	Initialization (with StorageConfig)
2.	Returning a list of samples with experiments (based on filename pattern matching)
3.	Read a file (capable of continuing an interrupted read)
4.	Write a file (capable of continuing an interrupted write)

# Debugging and testing
The initial implementation will be finalized after creating accounts of each type and testing all methods for each concrete class.
We should keep some data for all classes to make sure that future changes will not break the functionality.
This last step should be solved by using `pytest`.
As the changes could span several classes, and Python code does not require an explicit build process, the tests will be triggered manually.

# Contribute
Everyone is invited to add support for more storage classes (like Google Storage, Microsoft OneDrive, Google Drive, etc.)

If you want to learn more about creating good readme files then refer the following [guidelines](https://docs.microsoft.com/en-us/azure/devops/repos/git/create-a-readme?view=azure-devops). You can also seek inspiration from the below readme files:
- [ASP.NET Core](https://github.com/aspnet/Home)
- [Visual Studio Code](https://github.com/Microsoft/vscode)
- [Chakra Core](https://github.com/Microsoft/ChakraCore)