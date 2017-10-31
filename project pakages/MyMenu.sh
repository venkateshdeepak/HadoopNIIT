#!/bin/bash 
show_menu()
{
    NORMAL=`echo "\033[m"`
    MENU=`echo "\033[36m"` #Blue
    NUMBER=`echo "\033[33m"` #yellow
    FGRED=`echo "\033[41m"`
    RED_TEXT=`echo "\033[31m"`
    ENTER_LINE=`echo "\033[33m"`
    echo -e "${MENU}**********************H1B APPLICATIONS***********************${NORMAL}"
    echo -e "${MENU}**${NUMBER} 1) ${MENU} Is the number of petitions with Data Engineer job title increasing over time?${NORMAL}"
    echo -e "${MENU}**${NUMBER} 2) ${MENU} Find top 5 job titles who are having highest average growth in applications. ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 3) ${MENU} Which part of the US has the most Data Engineer jobs for each year? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 4) ${MENU} find top 5 locations in the US who have got certified visa for each year.${NORMAL}"
    echo -e "${MENU}**${NUMBER} 5) ${MENU} Which industry (SOC) has the most number of Data Scientist positions?${NORMAL}"
    echo -e "${MENU}**${NUMBER} 6) ${MENU} Which top 5 employers file the most petitions each year? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 7) ${MENU} Find the most popular top 10 job positions for H1B visa applications for each year (all applications)?${NORMAL}"
    echo -e "${MENU}**${NUMBER} 8) ${MENU} Find the most popular top 10 job positions for H1B visa applications for each year (only certified applications)?${NORMAL}"
    echo -e "${MENU}**${NUMBER} 9) ${MENU} Find the percentage and the count of each case status on total applications for each year. Create a graph depicting the pattern of All the cases over the period of time.${NORMAL}"
    echo -e "${MENU}**${NUMBER} 10) ${MENU} Create a bar graph to depict the number of applications for each year${NORMAL}"
    echo -e "${MENU}**${NUMBER} 11) ${MENU}Find the average Prevailing Wage for each Job for each Year (take part time and full time separate) arrange output in descending order${NORMAL}"
    echo -e "${MENU}**${NUMBER} 12) ${MENU} Which are employers who have the highest success rate in petitions more than 70% in petitions and total petions filed more than 1000?${NORMAL}"
    echo -e "${MENU}**${NUMBER} 13) ${MENU} Which are the top 10 job positions which have the  success rate more than 70% in petitions and total petitions filed more than 1000? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 14) ${MENU}Export result for option no 13 to MySQL database.${NORMAL}"
    echo -e "${MENU}*********************************************${NORMAL}"
    echo -e "${ENTER_LINE}Please enter a menu option and enter or ${RED_TEXT}enter to exit. ${NORMAL}"
    read opt
}
function option_picked() 
{
    COLOR='\033[01;31m' # bold red
    RESET='\033[00;00m' # normal white
    MESSAGE="$1"  #modified to post the correct option selected
    echo -e "${COLOR}${MESSAGE}${RESET}"
}
clear
show_menu
	while [ opt != '' ]
    do
    if [[ $opt = "" ]]; then 
            exit;
    else
        case $opt in

        1) clear;
        option_picked "1 a) Is the number of petitions with Data Engineer job title increasing over time?";
	
		hadoop jar nyse.jar q3project /niit2/h1b.csv /niit2/outt

		hadoop fs -cat /niit5/projop3/p*
	
        show_menu;
        ;;

        2) clear;
        option_picked "1 b) Top 5 job titles who are having highest average growth in applications";

		pig -x local 1bfinal.pig
	
        show_menu;
        ;; 
        3) clear;
        option_picked "2 a) Which part of the US has the most Data Engineer jobs for each year?";
		echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
		read var
		echo "part of the US has the most Data Engineer jobs for this year";
	    hive -e "use finalproject; SELECT split(worksite,'[,]')[1] as state,COUNT(case_status) AS number_of_petition,year from h1b_final WHERE job_title LIKE 'DATA ENGINEER' and year='2011' GROUP BY split(worksite,'[,]')[1], year ORDER BY number_of_petition desc limit 1;"
        show_menu;
        ;;

	4) clear;
        option_picked "2 b) find top 5 locations in the US who have got certified visa for each year.";
        	echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
		read var
             echo "top 5 locations in the US who have got certified visa for this year";
	    hive -e "use finalproject;  select worksite,count(case_status) as t,year from h1b_applications where year ='2011' and case_status='CERTIFIED' group by worksite,year order by t desc limit 5;"
       show_menu;
        ;;  

	5) clear;
        option_picked "3) Which industry has the most number of Data Scientist positions?";
             echo "The Industry which has the most number of Data Scientist positions:";
	    hadoop jar nyse.jar q3project /niit2/h1b.csv /niit2/outt3
	    hadoop fs -cat /niit2/projop4/p*
       show_menu;
        ;;

        6) clear;
        option_picked "4)Which top 5 employers file the most petitions each year?";
             echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
		read var
             echo "top 5 employers files most petitions for this year";
	    hive -e "use finalproject;  select employer_name, year, count(employer_name) total from h1b_final where year = '2011' and case_status = 'CERTIFIED' group by employer_name, year order by total desc limit 5;"
         show_menu;
        ;;

        7) clear;
        option_picked "5a) Find the most popular top 10 job positions for H1B visa applications for each year (all applications)?";
		echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
		read var
             echo "top 10 job positions for this year";
	    hive -e "use finalproject;  select job_title,year,count(case_status) as temp from h1b_applications where     year = '2011' group by job_title,year  order by temp desc limit 10;"
        show_menu;
        ;;

        8) clear;
        option_picked "5b) Find the most popular top 10 job positions for H1B visa applications for each year (only certified applications)?";
		echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
		read var
             echo "top 10 job positions who are certified for this year";
	    hive -e "use finalproject;  select job_title,year,count(case_status ) as temp from h1b_applications where     year = 2011 and case_status='CERTIFIED' group by job_title,year  order by temp desc limit 10; "
        show_menu;
        ;;

        9) clear;
		option_picked "6) Find the percentage and the count of each case status on total applications for each year. Create a graph depicting the pattern of All the cases over the period of time.";
              echo "The percentage and the count of each case status on total applications for each year:";
	    hive -e "use finalproject; select round((SUM(case case_status when 'CERTIFIED' then 1 end)/count(case_status))*100,2) as CERTIFIED, round((SUM(case case_status when 'CERTIFIED-WITHDRAWN' then 1 end)/count(case_status))*100,2) as CERTIFIED_WITHDRAWN, round((SUM(case case_status when 'WITHDRAWN' then 1 end)/count(case_status))*100,2) as WITHDRAWN, round((SUM(case case_status when 'DENIED' then 1 end)/count(case_status))*100,2) as DENIED, year from h1b_final group by year order by year;"
        show_menu;
        ;;

	10) clear;
        option_picked "7) Create a bar graph to depict the number of applications for each year";
              echo "The number of applications for each year:";
	    hive -e "use finalproject; select year, count(*) from h1b_final group by year;"
        show_menu;
        ;;

	11) clear;
        option_picked "8) Find the average Prevailing Wage for each Job for each Year (take part time and full time separate) arrange output in descending order";
        echo -e "${MENU}Select One Option From Below${NORMAL}"
        echo -e "${MENU}**${NUMBER} 1)${MENU} Full time job${NORMAL}"
        echo -e "${MENU}**${NUMBER} 2)${MENU} Part time job ${NORMAL}"
	        read n
	    case $n in
                1)	echo "Full time"
                    hive -e "use finalproject; select job_title,full_time_position,year,avg(prevailing_wage) as average from h1b_applications   where full_time_position ='Y' and year='2011' group by job_title,full_time_position,year order by average desc;"
                    ;;		
                    
                2) 	echo "Part time"
                   hive -e "use finalproject; select job_title,full_time_position,year,avg(prevailing_wage) as average from h1b_applications   where full_time_position ='N' and year='2011' group by job_title,full_time_position,year order by average desc;"
                    ;;
 
                *) echo "Please Select one among the option[1 or 2]";;
                esac
        show_menu;
        ;;

	12) clear;
	option_picked "9) Which are   employers who have the highest success rate in petitions more than 70% in petitions and total petions filed more than 1000?"
		pig -x local 9final.pig
        show_menu;
        ;;
	
	13) clear;
	option_picked "10) Which are the top 10 job positions which have the  success rate more than 70% in petitions and total petitions filed more than 1000?"
		pig -x local 10final.pig
        show_menu;
        ;;

	14) clear;
	option_picked "11) Export result for question no 10 to MySql database."
		

		mysql -u root -p'12345' -e 'select * from question11.question11';
        show_menu;
        ;;

		\n) exit;   
	;;
        
	*) clear;
        option_picked "Pick an option from the menu";
        show_menu;
        ;;
    esac
fi
done


