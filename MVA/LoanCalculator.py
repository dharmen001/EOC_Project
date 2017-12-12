strloanAmount = input("How much money will you borrow?" + " ")
strinterestRate = input("What is the intrest rate on the loan?" + " ")
strloanDurationInYears = input("How many years will it take you to pay off the load?" + " ")


loanDurationInYears = float(strloanDurationInYears)
loanAmount = float(strloanAmount)
interestRate = float(strinterestRate)


numberOfPayments = loanDurationInYears*12

monthlyPayment = loanAmount*interestRate*(1+interestRate)*numberOfPayments \
                 /((1+interestRate) * numberOfPayments - 1)

print ("your monthly payment will be " + str(monthlyPayment))

print ("your monthly payment will be $%.2f" % monthlyPayment)