#!/usr/bin/python
#### static vars ####
SI=2
daily_energy=66667
ECE=0.003
#### vars input ####
elders=int(input('How many elders are in Your family? : '))
adults=int(input('How many adults are in Your family? : '))
kids=int(input('How many kids do You have?  : '))
work=input('Do You work with powerpoint, hoe or in a quarry? 
(Type "powerpoint", "hoe" or "quarry") : ')
field_area=int(input('How big is Your field (in square meters)? : '))
#### calculation ####
if work == "powerpoint":
        daily_energy=44444
elif work == "hoe":
        daily_energy=66667
elif work == "quarry":
        daily_energy=93333
else:
        print ('While You made mistake in type of work I assume 
        average energy output, working with hoe, so:')
        daily_energy=66667
FEI=((elders*65) + (adults*70) + (kids*20)) * (daily_energy) / 1000000
HH=round((FEI)/(SI)/(ECE))
surplus=round(field_area/HH)
#### final condition and results ####
if field_area <= HH:
        print ('Well, Your field is too small, You need {} sq. 
        meters at least.'.format(HH))
elif HH <= field_area <=(2*HH):
        print('OK, Your field is big enough - the limit is {} sq. 
        meters, but I recommend You to buy some more.'.format(HH))
else :
        print ('Your field is  {}. times bigger than needed. 
        Trust in God but keep Your power dry.'.format(surplus))
exit()
