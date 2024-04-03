//***************************************************************
//
//  Developer:     <Your full name>
//
//  Program #:     <Assignment Number>
//
//  File Name:     <IntegerWithSquareRoot.java>
//
//  Course:        COSC-3365 Distributed Databases using Hadoop
//
//  Due Date:      <Due Date>
//
//  Instructor:    Fred Kumi 
//
//  Hadoop Topic:  Apache Spark
//
//  Description:   <An explanation of what the program is designed to do>
//
//***************************************************************

package TuplesPackage;

public class IntegerWithSquareRoot
{
	private int originalNumber;
	private double squareRoot;
	
	// Constructor
	public IntegerWithSquareRoot(int value)
	{
		this.originalNumber = value;
		this.squareRoot = Math.sqrt(originalNumber);
	}
	
	public int getOriginalNumber()
	{
		return originalNumber;
	}
	
	public double getSquareRoot()
	{
		return squareRoot;
	}
}
