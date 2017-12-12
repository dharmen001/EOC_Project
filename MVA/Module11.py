fileName = "C:/Users/Dharmendra.Mishra/Box Sync/Dharamendra Reports/John Request/sales.2017.01.31.v2.cumulative.csv"
accessmode = 'w'
myFile = open(fileName,mode=accessmode)
for index in range(2):
    name = input ( "Enter the name" )
    age = input("Enter the age")
    myFile.write(name +","+age + "\n")
myFile.close()


