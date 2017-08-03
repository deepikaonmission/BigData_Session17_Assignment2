/**
Datafile is "17.2_Dataset.txt"

NOTE: In uploaded 17.2_Dataset.txt file, there are five fields, so I asked support team, what is the role of fifth
  field here, because in assignment 17.2, description of four fields is given i.e. (name,subject,grade,marks)
  And support team in response to my email said there are four fields itself.
  Therefore, in the below code I am using only four fields discarding fifth field, as I didn't get the meaning of fifth field.
  Below is the data which I used in the following code:

Mathew,science,grade-3,45
Mathew,history,grade-2,55
Mark,maths,grade-2,23
Mark,science,grade-1,76
John,history,grade-1,14
John,maths,grade-2,74
Lisa,science,grade-1,24
Lisa,history,grade-3,86
Andrew,maths,grade-1,34
Andrew,science,grade-3,26
Andrew,history,grade-1,74
Mathew,science,grade-2,55
Mathew,history,grade-2,87
Mark,maths,grade-1,92
Mark,science,grade-2,12
John,history,grade-1,67
John,maths,grade-1,35
Lisa,science,grade-2,24
Lisa,history,grade-2,98
Andrew,maths,grade-1,23
Andrew,science,grade-3,44
Andrew,history,grade-2,77

NOTE: The Problem Statements can be solved using SparkSql as well, but I have created code here considering Class 17 only
  */

import org.apache.spark.{SparkConf, SparkContext}    //As this is Spark program so import of SparkConf, SparkContext is required
                                            //but import of spark, SparkConf, SparkContext will be available if libraries are specified in
                                            //build.sbt file which will automatically import the library dependencies based on scala and spark version if specified

object Session17Assignment2New extends App {    //singleton class

  val conf = new SparkConf().setAppName("Session17Assign2New").setMaster("local")
  //SparkConf allows to configure some of the common properties like master URL and application name
  //setMaster sets Master URL which is local in this case,
  //setAppName sets Application name here application name is "Sesison17Assign1" which can be helpful in case of debugging
  //finally these configuration settings are passed to "conf" object of SparkConf

  val sc = new SparkContext(conf)
  //SparkContext constructor is passed a SparkConf object (i.e. conf) which contains information about our application
  //now from here on, we can work on SparkContext using sc


  /******************************* PROBLEM STATEMENT 1 STARTS **********************************
  Problem Statement 1:
  1. Read the text file, and create a tupled rdd.
  2. Find the count of total number of rows present.
  3. What is the distinct number of subjects present in the entire school
  4. What is the count of the number of students in the school, whose name is Mathew and marks is 55*/


  //1. Read the text file, and create a tupled rdd.
  //Reading text file by specifying the location inside sc.textFile(file:///directory/filename) this creates RDD i.e.
  //firstRdd but data of RDD is not available until an action is performed on this RDD
  val firstRdd = sc.textFile("file:///G:/ACADGILD/course material/Hadoop/Sessions/Session 17/Assignments/Assignment2/17.2_Dataset.txt")

  firstRdd.foreach(x => println(x))     //foreach is an action that is performed on firstRdd and it displays each line of data held by firstRdd

  //REFER SCREENSHOT ProblemStatement1_Problem1_1.1 FOR OUTPUT

  val tupledRdd = firstRdd.map(line => {                     //tupledRdd -->> RDD[(String,String,String,String,String)]
    val strings = line.split(",")
    (strings(0),strings(1),strings(2),strings(3),strings(4))
  })
  // in above code, map transformation is applied on firstRdd, where each line of data of firstRdd is taken,
  //and split using ",", then new variable "strings" holds split data,
  //finally tupledRdd RDD[(String,String,String,String,String)] is created on the basis of last statement i.e. (strings(0),strings(1),strings(2),strings(3),strings(4))
  // which is a tuple of five strings

  println("<<<<<<<<<<<<---------- TUPLED RDD ------------>>>>>>>>>>>>")
  tupledRdd.foreach(x => println(x))           //foreach action is performed on tupledRdd and it prints each line of tupledRdd

  //REFER SCREENSHOT ProblemStatement1_Problem1_1.2 FOR OUTPUT




  //2. Count of total number of rows present
  val rowsCount = tupledRdd.count()         //rowsCount -->> Long
  //Here, count() action is performed on tupledRdd which returns Long value i.e. total number of rows present in tupledRdd

  println("Total number of rows present : "+rowsCount)      //prints specified string and value of rowsCount

  //REFER SCREENSHOT ProblemStatement1_Problem2 FOR OUTPUT


  //3. Distinct number of subjects present in the entire school

  val distinctSub = tupledRdd.toLocalIterator.map(tup => tup._2).toStream.distinct   //distinctSub -->> Stream[String]
  //here, tupledRdd is first converted to LocalIterator
  //then, map operation is applied on this Iterator, where only second field of tupled is fetched from each set of tuples
  //then mapped result is converted to Stream, because while working with iterator, if foreach is used then it takes the iterator
  //to its end, so after using foreach if we perform any operation like map, filterMap, length, size etc. on the Iterator
  //then no result is shown, however this doesn't happen with Stream
  //so at the end, Stream of string is created i.e. distinctSub

  println("<<<<<<<<--------- Distinct Subjects in entire School are as follows --------->>>>>>>>>>")
  distinctSub.foreach(x => println(x))        //Distinct subjects are printed
  //REFER SCREENSHOT ProblemStatement1_Problem3 FOR OUTPUT

  val countDistSub = distinctSub.length       //length of Stream is returned in countDistSub
  println("Distinct Number of Subjects present in entire school are : "+countDistSub) //prints specified string and value of countDistSub
  //REFER SCREENSHOT ProblemStatement1_Problem3 FOR OUTPUT


  //4. count of the number of students in the school, whose name is Mathew and marks is 55

  val students = tupledRdd.toLocalIterator.filter(tup => (tup._1 == "Mathew" && tup._4.toInt == 55)).toStream  //students -->> Stream[(String,String,String,String,String)]
  //here tupledRdd is first converted to LocalIterator
  //then filter operation is applied on this Iterator, which filters only those tuples where name of student is "Mathew" and marks is "55"
  //then filtered result is converted to Stream, reson is explained above
  //and Stream of [(String,String,String,String,String)] is createad i.e. students

  println("<<<<<----- Students in the school, whose name is Mathew and marks is 55 ----->>>>>")
  students.foreach(x => println(x))      //foreach is applied on students where each row of it, is printed
  //REFER SCREENSHOT ProblemStatement1_Problem4 FOR OUTPUT

  println("Count of the number of students in the school, whose name is Mathew and marks is 55 : "+students.length)  //prints specified string and value of students.length (student.length returns total row)
  //REFER SCREENSHOT ProblemStatement1_Problem4 FOR OUTPUT

  /******************************* PROBLEM STATEMENT 1 ENDS ***********************************/


  /******************************* PROBLEM STATEMENT 2 STARTS ***********************************
  1. What is the count of students per grade in the school?
  2. Find the average or average score of each student (Note - Mathew is grade-1, is different from Mathew in some other grade!)
  3. What is the average score of students in each subject across all grades?
  4. What is the average score of students in each subject per grade?
  5. For all students in grade-2, how many have average score greater than 50?  */

  // 1. Count of students per grade in the school

  val countStdPerGrade = tupledRdd.groupBy(_._3)     //countStdPerGrade -->>  RDD[(String, Iterable[(String,String,String,String,String)])]
  //here, groupBy transformation is applied on tupledRdd on the basis of grade (_ means a set of tuples and _._3 means third field inside each tuple)
  //which creates a new RDD of (String, Iterable[(String,String,String,String,String)]) i.e. countStdPerGrade

  val countStdPerGrade1 = countStdPerGrade.sortBy(_._1).map(x => (x._1,x._2.toSeq.length))    //countStdPerGrade1 -->> RDD[(String,Int)]
  // here, sortBy transformation is applied on countStdPerGrade RDD on the basis of first element inside tuple i.e. grade
  //then map transformation is applied on sorted result, where each tuple is taken and results a tuple of two elements
  //where x._1 means first field of tuple i.e. grade and x._2.toSeq.length means length of second field of tuple which is an Iterable of String
  //therefore, x._2 is first converted to Sequence so that length can directly be computed which is not possible in case of Iterable
  //i.e. length can't directly be used with Iterable, i.e. x._2.length
  //finally a new RDD [(String, Int)] is created i.e. countStdPerGrade1 in the form of (grade, total students)

  println("<<<<---- Count of students per grade in the school are as follows ---->>>>")
  countStdPerGrade1.foreach(x => println(x))       //foreach action is applied on countStdPerGrade1 RDD where each row is printed
  //REFER SCREENSHOT ProblemStatement2_Problem1 FOR OUTPUT


  //2. Find the average of each student (Note - Mathew is grade-1, is different from Mathew in some other grade!)

  val avgStd = tupledRdd.groupBy(x => (x._1,x._3))   //avgStd -->> RDD[((String,String),Iterable[(String,String,String,String,String)])]
  //Here, groupBy transformation is applied on tupledRdd, on the basis of two fields of tuple i.e. name (x._1) and grade (x._3)
  //so new RDD [((String,String),Iterable[(String,String,String,String,String)])] i.e. avgStd is created

  val avgStd1 = avgStd.map(x => (x._1._1,x._1._2,x._2.map(x => x._4).toList.map(x => x.toInt))).sortBy(_._1) //avgStd1 -->> RDD[(String,String,List[Int])]
  //Now map transformation is applied on avgStd RDD, where each element of RDD containing tuple is taken
  //and resultant RDD avgStd1 contains tuple of "name (x._1._1 accessing field inside tuple of tuple)", "grade (x._1._2)"
  //and on third field of tuple i.e. x._2 which is Iterable[(String,String,String,String,String)])], map operation is applied
  //which results only fourth field of tuple i.e. marks(x._4), which is further converted to List, and further on List,
  //map operation is applied to convert each element inside List to Int,
  //then sortBy opertion is applied on the basis of name(_._1) field of tuple
  //finally avgStd1 RDD[(String,String,List[Int])] is created i.e. RDD of tuple of name, grade,List[marks]
  //NOTE: Iterable is converted to List in order to perform maths operation and to convert String to Int,
  //because with Iterable conversion from String to Int is difficult to achieve


  //avgStd1.foreach(x => println(x))       //foreach action works on avgStd1 RDD and prints each element inside it

  val avgStd2 = avgStd1.map(x => (x._1,x._2,x._3.reduce(_+_),x._3.length))    //avgStd2 -->> RDD[(String,String,Int,Int)]
  //here map transformation is applied on avgStd1 RDD where, tuple of name, grade, and sum of marks, and length of List[marks] is returned

  avgStd2.foreach(x => println(x))         //foreach action works on avgStd2 RDD and prints each element inside it
  //REFER SCREENSHOT ProblemStatement2_Problem2_2.1 FOR OUTPUT

  val avgStd3 = avgStd2.map(x => (x._1,x._2,x._3.toFloat/x._4.toFloat))    //avgStd3 -->> RDD[(String,String,Float)]
  //map transformation is applied on avgStd2 RDD, which results tuple of name, grade and average of marks
  //because x._3.toFloat sum of marks as Float and x._4.toFloat is length of List[marks] as Float
  //so division of these two results average of marks

  println("<<<<---- Average of each student per grade (Note - Mathew is grade-1, is different from Mathew in some other grade!) ---->>>>")
  avgStd3.foreach(x => println(x))    //foreach action works on avgStd3 RDD and prints each element inside it
  //REFER SCREENSHOT ProblemStatement2_Problem2_2.2 FOR OUTPUT



  //3. What is the average score of students in each subject across all grades?
  val avgScoreStd = tupledRdd.groupBy(x => (x._1,x._2))  //avgScoreStd -->> RDD[((String,String),Iterable[(String,String,String,String,String)])]
  //here, grouping is done on the basis of name and subject as rest explanation is same as above
  val avgScoreStd1 = avgScoreStd.map(x => (x._1._1,x._1._2,x._2.map(x => x._4).toList.map(x => x.toInt))).sortBy(_._1) //avgScoreStd1 -->> RDD[(String,String,List[Int])]
  //avgScoreStd1.foreach(x => println(x))

  val avgScoreStd2 = avgScoreStd1.map(x => (x._1,x._2,x._3.reduce(_+_),x._3.length)) //avgScoreStd2 -->> RDD[(String,String,Int,Int)]
  avgScoreStd2.foreach(x => println(x))
  //REFER SCREENSHOT ProblemStatement2_Problem3_3.1 FOR OUTPUT

  val avgScoreStd3 = avgScoreStd2.map(x => (x._1,x._2,x._3.toFloat/x._4.toFloat))   ////avgScoreStd3 -->> RDD[(String,String,Float)]
  println("<<<<---- Average score of students in each subject across all grades ---->>>>")
  avgScoreStd3.foreach(x => println(x))
  //REFER SCREENSHOT ProblemStatement2_Problem3_3.2 FOR OUTPUT


  //4. What is the average score of students in each subject per grade?
  val avgScoreSubGrade = tupledRdd.groupBy(x => (x._1,x._2,x._3))    //avgScoreSubGrade -->> RDD[((String,String,String),Iterable[(String,String,String,String,String)])]
  //here, grouping is done on the basis of name and subject and rest explanation is same as above

  val avgScoreSubGrade1 = avgScoreSubGrade.map(x => (x._1._1,x._1._2,x._1._3,x._2.map(x => x._4).toList.map(x => x.toInt))).sortBy(x => (x._1,x._2))  //avgScoreSubGrade1 -->> RDD[(String,String,String,List[Int])]
  //Sorting is done on the basis of name and subject
  //avgScoreSubGrade1.foreach(x => println(x))

  val avgScoreSubGrade2 = avgScoreSubGrade1.map(x => (x._1,x._2,x._3,x._4.reduce(_+_),x._4.length)) //avgScoreSubGrade2 -->> RDD[(String,String,String,Int,Int)]
  avgScoreSubGrade2.foreach(x => println(x))
  //REFER SCREENSHOT ProblemStatement2_Problem4_4.1 FOR OUTPUT

  val avgScoreSubGrade3 = avgScoreSubGrade2.map(x => (x._1,x._2,x._3,x._4.toFloat/x._5.toFloat))   ////avgScoreSubGrade3 -->> RDD[(String,String,String,Float)]
  println("<<<<---- Average score of students in each subject per grade ---->>>>")
  avgScoreSubGrade3.foreach(x => println(x))
  //REFER SCREENSHOT ProblemStatement2_Problem4_4.2 FOR OUTPUT


  //5. For all students in grade-2, how many have average score greater than 50?
  val avgScoreGrade2Great50 = tupledRdd.groupBy(x => (x._1,x._3)).filter(x=> x._1._2=="grade-2")    //avgScoreGrade2Great50 -->> RDD[((String,String),Iterable[(String,String,String,String,String)])]
  //here, grouping is done on the basis of name and grade
  // then filter operation is applied to fetch only those rows which contain "grade-2"
  // and rest explanation is same as above

  val avgScoreGrade2Great501 = avgScoreGrade2Great50.map(x => (x._1._1,x._1._2,x._2.map(x => x._4).toList.map(x => x.toInt))).sortBy(x => (x._1,x._2))  //avgScoreGradeGreat501 -->> RDD[(String,String,List[Int])]
  //avgScoreGrade2Great501.foreach(x => println(x))

  val avgScoreGrade2Great502 = avgScoreGrade2Great501.map(x => (x._1,x._2,x._3.reduce(_+_),x._3.length)) //avgScoreGrade2Great502 -->> RDD[(String,String,Int,Int)]
  avgScoreGrade2Great502.foreach(x => println(x))
  //REFER SCREENSHOT ProblemStatement2_Problem5_5.1 FOR OUTPUT

  val avgScoreGrade2Great503 = avgScoreGrade2Great502.map(x => (x._1,x._2,x._3.toFloat/x._4.toFloat)).filter(x => x._3>50.toFloat)   ////avgScoreGrade2Great503 -->> RDD[(String,String,Float)]
  //filter operation filters only those records with average greater than 50
  println("<<<<---- Students having grade-2 and average score greater than 50 ---->>>>")
  avgScoreGrade2Great503.foreach(x => println(x))
  //REFER SCREENSHOT ProblemStatement2_Problem5_5.2 FOR OUTPUT

  /******************************* PROBLEM STATEMENT 2 ENDS ***********************************/



  /******************************* PROBLEM STATEMENT 3 STARTS ***********************************/

  /*
    Are there any students in the college that satisfy the below criteria :
    1. Average score per student_name across all grades is same as average score per student_name per grade
    Hint - Use Intersection Property.

    //Two cases are created to solve answer this query
 */

  //<<<<<<<<<<<<<<<<<<<<<<<<<--------------- CASE 1 --------------->>>>>>>>>>>>>>>>>>>>>>>>>

  //Finding "Average Score per student_name across all grades"
  val avgScorePerStdName = tupledRdd.groupBy(x => (x._1))    //avgScorePerStdName -->> RDD[(String,Iterable[(String,String,String,String,String)])]
  //avgScorePerStdName.foreach(x => println(x))

  val avgScorePerStdName1 = avgScorePerStdName.map(x => (x._1,x._2.map(x => x._4).toList.map(x => x.toInt))).sortBy(_._1)  //avgScorePerStdName1 -->> RDD[(String,List[Int])]
  //avgScorePerStdName1.foreach(x => println(x))

  val avgScorePerStdName2 = avgScorePerStdName1.map(x => (x._1,x._2.reduce(_+_),x._2.length)) //avgScorePerStdName2 -->> RDD[(String,Int,Int)]
  //avgScorePerStdName2.foreach(x => println(x))

  val avgScorePerStdName3 = avgScorePerStdName2.map(x => (x._1,x._2.toFloat/x._3.toFloat)) //avgScorePerStdName3 -->> RDD[(String,Float)]
  println("<<<<<<------ Average Score per student_name across all grades ------>>>>>>")
  avgScorePerStdName3.foreach(x => println(x))
  //REFER SCREENSHOT ProblemStatement3_Problem1_CASE1_1.1 FOR OUTPUT


  //Finding Average score per student_name per grade      use avgStd3 RDD, which has three fields in RDD of tuple
  //to apply intersection operation, number and type of fields must be same in both RDDs
  //in avgScorePerStdName3 RDD, there are two fields in tuple, therefore, below new RDD is created with two fields inside each tuple


  val avgStd3New = avgStd3.map(x => (x._1,x._3))       //avgStd3New -->> RDD[(String,Float)]
  println("<<<<<<------ Average score per student_name per grade ------>>>>>>")
  avgStd3New.foreach(x => println(x))
  //REFER SCREENSHOT ProblemStatement3_Problem1_CASE1_1.2 FOR OUTPUT

  //using intersection operation to find common records in both RDDs
  val intersectResult = avgScorePerStdName3.intersection(avgStd3New)     //intersectResult -->> RDD[(String,Float)]

  //if intersectResult is empty then "No Such record found" msg is displayed
  if(intersectResult.isEmpty()) println("<<<<<<------ No such record found -------->>>>>>")
    //else common records are printed
  else intersectResult.foreach(x => println(x))
  //REFER SCREENSHOT ProblemStatement3_Problem1_CASE1_1.3 FOR OUTPUT

  //*************************** CASE 1 ENDS **************************

  //<<<<<<<<<<<<<<<<<<<<<<<<<--------------- CASE 2 --------------->>>>>>>>>>>>>>>>>>>>>>>>>

  //Average score of students in each subject across all grades          using avgScoreStd3
  avgScoreStd3.foreach(x => println(x))
  //REFER SCREENSHOT ProblemStatement3_Problem1_CASE2_1.1 FOR OUTPUT

  //Average score per student_name per grade              using avgStd3
  avgStd3.foreach(x => println(x))
  //REFER SCREENSHOT ProblemStatement3_Problem1_CASE2_1.2 FOR OUTPUT

  //Since avgScoreStd3 contains three fields, i.e. name, subject, marks
  //and avgStd3 contains three fields, i.e. name, grade, marks
  //therefore, to use intersection operation, two fields are fetched from both RDDs using map operation in order to fetch common records

  val intersectResult1 = avgScoreStd3.map(x => (x._1,x._3)).intersection(avgStd3.map(x => (x._1,x._3)))
  //if intersectResult1 is empty then "No Such record found" msg is displayed
  if(intersectResult1.isEmpty()) println("<<<<<<------ No such record found -------->>>>>>")
  //else common records are printed
  else intersectResult1.foreach(x => println(x))
  //REFER SCREENSHOT ProblemStatement3_Problem1_CASE2_1.3 FOR OUTPUT

  //*************************** CASE 2 ENDS **************************

  /******************************* PROBLEM STATEMENT 3 ENDS ***********************************/

}
