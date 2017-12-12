fileName = "C:/Users/Dharmendra.Mishra/Box Sync/Dharamendra Reports/John Request/sales.2017.03.27.v2.cumulative.csv"
READ = 'r'
myFile = open(fileName, READ)
allContents = myFile.readlines()
print (allContents)
