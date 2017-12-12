strTotalPurchase = 0
shipping = 0
strTotalPurchase = input("Enter the amount of total purchase: ")
totalPurchase = float(strTotalPurchase)
shipping = 10
if totalPurchase<=50:
    totalPurchase = totalPurchase+shipping
    print (totalPurchase)
else:
    totalPurchase
print("Final bill $%.2f  including shipping cost:" % totalPurchase)