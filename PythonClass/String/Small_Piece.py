import random
#list_a = [1,2,3,4,"Dharmendra",[2,4,5],7,9,10,23]
list_a = [random.randint(1,100) for i in range(random.randint(50,100))]
print list_a
size = int(input("Enter the no to make it in small\n"))

noOfPerfectGroup = len(list_a)/size
ramainingElementGroup = len(list_a)%size



for i in range(noOfPerfectGroup):
    print list_a[i*size: size*i + size]

print ramainingElementGroup