import datetime
currentDate = datetime.date.today()
strDeadline = input("Kindly give deadline date of project (mm/dd/yyyy):")
deadline = datetime.datetime.strptime(strDeadline,"%m/%d/%Y").date()
totalnbrDays = deadline - currentDate

nbrWeeks = totalnbrDays.days/7

nbrDays = totalnbrDays.days%7

print ("you have %d %d days week" % (nbrWeeks, nbrDays) + "until your deadline.")

