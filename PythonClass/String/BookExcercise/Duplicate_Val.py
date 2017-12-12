def duplicatetounique(mylist):
    new_list = set()
    for i in mylist:
        new_list.add(i)
    #print (list ( new_list ))
    return (list(new_list))



#print list(new_list)
x = duplicatetounique(["Dharmendra","Dharmendra",1,1,5,5,6,7])
print  x



"""list_duplicate = ["Dharmendra","Dharmendra",1,1,5,5,6,7]
new_list = set()
for i in list_duplicate:
    new_list.add(i)
print (list(new_list))"""




