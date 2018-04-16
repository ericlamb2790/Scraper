import com.amazonaws.http.HttpResponse;
import com.amazonaws.util.StringUtils;
import com.koushikdutta.urlimageviewhelper.UrlImageViewHelper;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

public class Scrape implements Comparator<ArrayList<String>> {

	private Connection conn = null;
	private boolean finished = false;
	private boolean CLICKED = false;
	private int NUMPAGES = 10; // NUMBER OF PAGES TO LOOK THROUGH - SCALABLE
	private int NUMLETTERS = 26;
	private int ITERATORPIC = 0; // ITERATOR FOR NEXT IMAGE CLICK
	private ArrayList<ArrayList<String>> MASTERAM = new ArrayList<ArrayList<String>>(NUMPAGES); // MASTER
																								// ARRAY
																								// OF
																								// ITEMS
//	CONFIG BOOLS
	private boolean parseCSV = true;
	private boolean scrapeAm = false;
	private boolean pushAmazon = false;
	private boolean exportAmCSV = false;
	
	private ArrayList<ArrayList<String>> MASTERCSV = new ArrayList<ArrayList<String>>(NUMPAGES);
	private ArrayList<String> SEARCHINDEX = new ArrayList<>();
	final String aChar = new Character((char) 64).toString(); // PLACEMENT TO
																// SEARCH
																// ALPHABET
																// KEYWORDS
	public static PrintWriter outLog;

	public Scrape() {
		// region AmazonSetSearchIndexArray
		SEARCHINDEX.add("Appliances");
		SEARCHINDEX.add("ArtsAndCrafts");
		SEARCHINDEX.add("Automotive");
		SEARCHINDEX.add("Baby");
		SEARCHINDEX.add("Beauty");
		SEARCHINDEX.add("Blended");
		SEARCHINDEX.add("Books");
		SEARCHINDEX.add("Collectibles");
		SEARCHINDEX.add("Electronics");
		SEARCHINDEX.add("Fashion");
		SEARCHINDEX.add("FashionBaby");
		SEARCHINDEX.add("FashionBoys");
		SEARCHINDEX.add("FashionGirls");
		SEARCHINDEX.add("FashionMen");
		SEARCHINDEX.add("FashionWomen");
		SEARCHINDEX.add("GiftCards");
		SEARCHINDEX.add("Grocery");
		SEARCHINDEX.add("HomeGarden");
		SEARCHINDEX.add("HealthPersonalCare");
		SEARCHINDEX.add("Industrial");
		SEARCHINDEX.add("KindleStore");
		SEARCHINDEX.add("LawnAndGarden");
		SEARCHINDEX.add("Luggage");
		SEARCHINDEX.add("MP3Downloads");
		SEARCHINDEX.add("Magazines");
		SEARCHINDEX.add("Merchants");
		SEARCHINDEX.add("MobileApps");
		SEARCHINDEX.add("Movies");
		SEARCHINDEX.add("Music");
		SEARCHINDEX.add("MusicalInstruments");
		SEARCHINDEX.add("OfficeProducts");
		SEARCHINDEX.add("PCHardware");
		SEARCHINDEX.add("PetSupplies");
		SEARCHINDEX.add("Software");
		SEARCHINDEX.add("SportingGoods");
		SEARCHINDEX.add("Tools");
		SEARCHINDEX.add("Toys");
		SEARCHINDEX.add("UnboxVideo");
		SEARCHINDEX.add("VideoGames");
		SEARCHINDEX.add("Wireless");
		SEARCHINDEX.add("Wine");
		// endregion

		try {
			outLog = new PrintWriter("ScrapeLog.txt", "UTF-8");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}

	public void DoScrape() {
		if (MASTERAM.size() > 0)
			MASTERAM.clear();
		ITERATORPIC = 0;
		// AWS ACCOUNT INFO BELOW
		String AWS_ACCESS_KEY_ID = "AKIAIQAG42NDBT7BHR4Q";
		String AWS_SECRET_KEY = "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";
		String ENDPOINT = "webservices.amazon.com";
		// HELPER TO SIGN REQUESTS
		SignedRequestsHelper helper;
		for (int S = 0; S < SEARCHINDEX.size(); S++) {

			for (int AlphaN = 0; AlphaN < NUMLETTERS; AlphaN++) {

				for (int i = 1; i <= NUMPAGES; i++) {
					// END OF FILE BREAK
					try {
						helper = SignedRequestsHelper.getInstance(ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_KEY);
					} catch (Exception e) {
						e.printStackTrace();
						return;
					}

					String requestUrl = null;

					Map<String, String> params = new HashMap<String, String>();

					// BELOW ARE THE PARAMETERS NEEDED FOR SEARCH
					String aChar = new Character((char) (65 + AlphaN)).toString(); // Alphabet
																					// Search
																					// param
					params.put("Service", "AWSECommerceService");
					params.put("Operation", "ItemSearch");
					params.put("AWSAccessKeyId", "AKIAIQAG42NDBT7BHR4Q");
					// AWS Associate Tag
					params.put("AssociateTag", "fuzwal-20");
					params.put("SearchIndex", SEARCHINDEX.get(S)); // Instead of
																	// 'Blended',
																	// can loop
																	// through
																	// multiple
																	// search
																	// indicies
					params.put("Keywords", aChar); // search.getText().toString());
													// //THE KEYWORD WE CAN
													// CHANGE TO ALTER SEARCh ie
													// ALPHABET
					params.put("ItemPage", Integer.toString(i)); // NEEDS TO
																	// ITERATE
																	// THROUGH
																	// PAGES FOR
																	// MORE
																	// PRODUCTS
					params.put("ResponseGroup", "Images,ItemAttributes,SalesRank"); // THE
																					// XML
																					// FILE
																					// REQUEST
																					// PARAMS

					// Below sends in the URL for the params needed to query the
					// XML
					requestUrl = helper.sign(params);

					// System.out.println("Signed URL: \"" + requestUrl + "\"");
					// Toast.makeText(MainActivity.this, requestUrl,
					// Toast.LENGTH_LONG).show();

					String uri = requestUrl;
					// Intent intent = new Intent(Intent.ACTION_VIEW,
					// Uri.parse(uri)); //Intent is Android Based to launch the
					// Uri Parser

					Tasker task = new Tasker(uri); // Passes our XML Link(uri)
													// to the XML parser in
													// Tasker
					ArrayList<ArrayList<String>> head = new ArrayList<ArrayList<String>>(10); // Array
																								// List
																								// that
																								// is
																								// received
																								// from
																								// Tasker.
																								// LIMITED
																								// TO
																								// 10
																								// RESULTS
																								// PER
																								// PAGE
																								// IF
																								// CATEGORY
																								// IS
																								// ALL
																								// !!!
					try {
						head = task.doInBackground(head); // Grabbing the array
															// returned from
															// Tasker
					} catch (Exception e) {
						e.printStackTrace();
					}

					// Log.d("myTag", "ADDING PAGE" + Integer.toString(i));

					// MASTER.addAll(head); //Adds array of items into master
					// list to house product information
					if (head != null) {
						if (head.size() == 0) // If the page has less than 5
												// products on the last page the
												// next page will most likely be
												// garbage or empty
						{
							try {
								Thread.sleep(500);
								// Log.d("Page ","Sleep 500");
							} catch (Exception e) {
								e.printStackTrace();
							}
							break;
						} else {
							MASTERAM.addAll(head);
						}
					} else {
						break;
					}

					// Log.d("myTag", Integer.toString(AlphaN));

				}

				// Below is the code for setting the imageview in android to
				// display the product\

				// startActivity(intent);

			}
		}

		MASTERAM = removeDuplicates(MASTERAM);
		Collections.sort(MASTERAM, new Comparator<ArrayList<String>>() {
			@Override
			public int compare(ArrayList<String> lhs, ArrayList<String> rhs) {
				// -1 - less than, 1 - greater than, 0 - equal, all inversed for
				// descending
				// return lhs.get(1).compareTo(rhs.get(1));
				int l, r = 0;
				try {
					l = Integer.parseInt(lhs.get(1));
				} catch (Exception e) {
					l = Integer.MAX_VALUE;
				}
				try {
					r = Integer.parseInt(rhs.get(1));
				} catch (Exception e) {
					r = Integer.MAX_VALUE;
				}
				// return Integer.parseInt(lhs.get(1))-
				// Integer.parseInt(rhs.get(1));
				return l - r;
			}
		});
	}

	private ArrayList<ArrayList<String>> removeDuplicates(ArrayList<ArrayList<String>> list) {

		// Store unique items in result.
		ArrayList<ArrayList<String>> result = new ArrayList<ArrayList<String>>();

		// Record encountered Strings in HashSet.
		HashSet<String> set = new HashSet<>();

		int dupes = 0;
		// Loop over argument list.
		for (int i = 0; i < list.size(); i++) {

			// If String is not in set, add it to the list and the set.
			if (!set.contains(list.get(i).get(0))) {
				result.add(list.get(i));
				set.add(list.get(i).get(0));
			} else {
				dupes++;
			}
		}
		// Log.d("DUPER:","DUPE DETECTED - " + String.valueOf(dupes));

		return result;
	}

	@Override
	public int compare(ArrayList<String> lhs, ArrayList<String> rhs) {
		return lhs.get(1).compareTo(rhs.get(1));
	}

	public ArrayList<ArrayList<String>> ParseCSV(String name) {
		ArrayList<ArrayList<String>> MASTERZ = new ArrayList<ArrayList<String>>();
		ArrayList<String> TESTARR = new ArrayList<String>();
		Reader in;
		try {
			in = new FileReader("csv/"+name+".csv");
			//in = new FileReader("csv/walmart.csv");
			Iterable<CSVRecord> records = CSVFormat.EXCEL.parse(in);
			for (CSVRecord record : records) {
				String columnOne = record.get(0);
				String delim = "[|]";
				String[] tokens = columnOne.split(delim);
				//System.out.println(Arrays.toString(tokens));
				try {
//					for(String test : tokens)
//					{
//						System.out.print(test+",");
//					}
				TESTARR.add(tokens[0]);
				TESTARR.add(tokens[1]);
				TESTARR.add(tokens[3]);
				TESTARR.add(tokens[4]);
				TESTARR.add(tokens[6]);
				TESTARR.add(tokens[21]);
				System.out.print(MASTERZ.size()+": ");
				System.out.println(Arrays.toString(TESTARR.toArray()));
				
				MASTERZ.add(TESTARR);
				TESTARR = new ArrayList<String>();
				}
				catch (ArrayIndexOutOfBoundsException exception) {
					// TODO Auto-generated catch block
					continue;
				}
				// String columnTwo = record.get(1);

			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//ExportCSV(MASTERZ);
		
		return MASTERZ;

	}

	public void ExportCSV(ArrayList<ArrayList<String>> MASTERZ) {

		// String Item = String.join(",",test.toString());

		try {
			FileWriter writer = new FileWriter("sto1.csv");
			for (int line = 0; line < MASTERZ.size(); line++) {

				List<String> test = MASTERZ.get(line);
				String Item = test.toString().replaceAll("[\\s\\[\\]]", "");
				writer.write(Item + "\n");
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private void InsertInto(ArrayList<ArrayList<String>> test222,int AFFILIATE) throws SQLException
	{
					for(int i = 0;i<test222.size();i++)
					{
						if(AFFILIATE == 1) //1 for SAS 2 For AMAZON
						{
							try
							{
							 String query = " insert into Affiliate_Products2 (ASIN, Title, Website, URL,ImageURL, Category)"
						             + " values (?, ?, ?, ?, ?, ?)";
		
				              // create the mysql insert preparedstatement
				              PreparedStatement preparedStmt = conn.prepareStatement(query);
				              preparedStmt.setString (1, test222.get(i).get(0).toString());
				              preparedStmt.setString (2, test222.get(i).get(1).toString());
				              preparedStmt.setString (3, test222.get(i).get(2).toString());
				              preparedStmt.setString (4, test222.get(i).get(3).toString());
				              preparedStmt.setString (5, test222.get(i).get(4).toString());
				              preparedStmt.setString (6, test222.get(i).get(5).toString());
				              preparedStmt.execute();
							}
						    catch (Exception e)
						    {
						    	
						        System.out.println(e.getMessage());
						    }
						}
						if(AFFILIATE == 2)
						{
							try
							{
								String query = " insert into Amazon_Products (ASIN, SalesRank, URL, ImageURL, Brand, Title)"
							                + " values (?, ?, ?, ?, ?, ?)";
		
				              // create the mysql insert preparedstatement
				              PreparedStatement preparedStmt = conn.prepareStatement(query);
				              preparedStmt.setString (1, test222.get(i).get(0).toString());
				              preparedStmt.setString (2, test222.get(i).get(1).toString());
				              preparedStmt.setString (3, test222.get(i).get(2).toString());
				              preparedStmt.setString (4, test222.get(i).get(3).toString());
				              preparedStmt.setString (5, test222.get(i).get(4).toString());
				              preparedStmt.setString (6, test222.get(i).get(5).toString());
				              preparedStmt.execute();
				              
							}
						    catch (Exception e)
						    {
						    	
						        System.out.println(e.getMessage());
						        if(e.getMessage().contains("Duplicate"))
						        {
						        	try
									{
										String query = " update Amazon_Products set SalesRank = ?, URL = ?, ImageURL = ?, Brand = ?, Title = ?"
									                + " where ASIN='" + test222.get(i).get(0).toString() + "'";
				
						              // create the mysql insert preparedstatement
						              PreparedStatement preparedStmt = conn.prepareStatement(query); 
						              preparedStmt.setString (1, test222.get(i).get(1).toString());
			                          preparedStmt.setString (2, test222.get(i).get(2).toString());
			                          preparedStmt.setString (3, test222.get(i).get(3).toString());
			                          preparedStmt.setString (4, test222.get(i).get(4).toString());
			                          preparedStmt.setString (5, test222.get(i).get(5).toString());
						              preparedStmt.execute();
						              

									}
						        	catch (Exception e2)
								    {
								    	
								        System.out.println(e2.getMessage());
								    }
						        	
						        }
						    }
						}
					}
	}
	private void startConnection() throws SQLException
	{
	    
	    String url = "jdbc:mysql://sbclusteridapp124214.cluster-cpxq7h6ywwyb.us-east-1.rds.amazonaws.com:3306/";
	    String dbName = "sbdbapp26732";
	    String driver = "com.mysql.jdbc.Driver";
	    String userName = "sbertrdsuser802";
	    String password = "SBRDSPass0234!093243!2";
	    
	   //String url = "jdbc:mysql://sbertserver566homo2324.cpxq7h6ywwyb.us-east-1.rds.amazonaws.com:3306/";
	   //String dbName = "sbertserver566homo2324";
	   //String driver = "com.mysql.jdbc.Driver";
	   //String userName = "sbertuser5homo2";
	   //String password = "SBertSuperHomo393";

	    try
	    {
	        Class.forName(driver).newInstance();
	        conn = DriverManager.getConnection(url+dbName,userName,password);
	        System.out.println("Connected to the database");
	        //conn.close();
	        //System.out.println("Disconnected from database");  
	        
	        //String query = " insert into TEST_AMAZONPRODUCTS (ASIN, Title, URL, ImageURL, SalesRank)"
	               // + " values (?, ?, ?, ?, ?)";
	        
	      //String query = " insert into TEST_AFFILIATEPRODUCTS (ProductID, Title, Website, URL ,ImageURL)"
             //+ " values (?, ?, ?, ?, ?)";

	              // create the mysql insert preparedstatement
	             // PreparedStatement preparedStmt = conn.prepareStatement(query);
	             // preparedStmt.setString (1, "10000");
	             // preparedStmt.setString (2, "FidgetSpinner");
	             // preparedStmt.setString (3, "www.fidgetspinner.com");
	             // preparedStmt.setString (4, "www.fidgetspinner.com/image.jpg");
	             // preparedStmt.setString (5, "1");
	            //  preparedStmt.execute();
	        
	    }
	    catch (Exception e)
	    {
	        System.out.println("NO CONNECTION =(");
	    }
	    
	   

	        // Now do something with the ResultSet ....
	}
	public static void main(String[] args) throws SQLException {
		System.out.println("Scrape starting...");
		Scrape test = new Scrape();
		test.startConnection();
		Scanner s = new Scanner(System.in);
		System.out.print("Enter CSV name: ");
		String csvname = s.next();
		test.MASTERCSV.addAll(test.ParseCSV(csvname));
			//test.MASTERCSV.addAll(test.ParseCSV());
	    test.InsertInto(test.MASTERCSV,1);
		
//		if(test.scrapeAm)
//		{
//			test.DoScrape();
//			System.out.println("Scrape completed!");
//		}
//		
//		if(test.pushAmazon)
//		{
//			test.InsertInto(test.MASTERAM,2);
//		}
//
//		if(test.exportAmCSV)
//		{
//			test.ExportCSV(test.MASTERAM);
//		}
		

	}
}
