orderTotal = 0
provinance = ""
country = ""
totalwithtax = 0
GST = .05
HST = .13
PST = .06
country = input("which country you are from?>> ")
if country.lower() == "canada" :
    province = input("Which province are you from? ")
orderTotal = float(input("What is your order total? "))

if country.lower() == "Canada":
    if provinance.lower() == "Alberta":
        orderTotal = orderTotal + orderTotal * GST
    elif provinance.lower() == "ontario" or provinance.lower() == "new brunswick" or provinance.lower() == "nova scotia" :
        orderTotal = orderTotal + orderTotal * HST
    else:
        ordertotal = orderTotal + orderTotal * PST + orderTotal * GST
print("your total including taxes $%.2f" % orderTotal)





