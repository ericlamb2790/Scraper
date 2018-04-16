

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

/**
 * Created by Eric Lam on 6/1/2017.
 */

//Passes in a Master ArrayList to be populated with each tag retrieved via parsing the XML file into sub-ArrayLists of strings
public class Tasker  {

    //The number of Item Info fields we're keeping from the parse. Scalable to handle as many fields as is needed
    final private int FIELDNUM = 6;

    //Tells the program when we've retrieved all results that are available, so we don't try to search for 1000 pages when there are only 50 pages of results
    static boolean eof = false;

    //How many cycles should be done between throttles
    final int numCycles = 2;
    //increments per loop for modulo operation
    int shouldThrottle = 0;
    
    int throttleMS = 140;

    //This is the array that holds all the items from this search. The outer array holds the items, the inner array is an array of item info strings
    //A list of lists of items. Can be simplified into a struct at a later point
    ArrayList<ArrayList<String>> headlines = new ArrayList<ArrayList<String>>();

    //This is the array of item information fields pulled from each parse. This is what we append to the master file in SearchActivity
    ArrayList<String> links = new ArrayList<String>(FIELDNUM);

    //The uri is the link to the xml file that we're pulling the data from
    String uri = "";

    //Opens the url to be accessed by our code
    //Gives us a stream of data
    public Reader getInputStream(URL url) {
        try {
        	
        	Reader reader = new InputStreamReader(url.openConnection().getInputStream(), StandardCharsets.UTF_8);
            return reader;
        } catch (Exception e) {
            return null;
        }
    }

    // Tasker is a multithreaded class that runs our scrape on a separate thread
    // The only reason this would need to be multithreaded is in the case that the scrape needs to be done real time in-app, which is not the case
    public Tasker(String s) {
        uri = s;
    }


    //Multithreading code that will be deprecated in the formal scraper--------------------------------

    protected void onPostExecute(ArrayList<ArrayList<String>> result) {
        //Log.d("TEST","FINISH");
    }

    public ArrayList<ArrayList<String>> RETURNLIST() {
        return headlines;
    }


    //--------------------------------------------------------------------------------------------------

    //This is the execution thread for our scrape, parsing happens here
    public ArrayList<ArrayList<String>> doInBackground(ArrayList<ArrayList<String>> params) {
    	int count = 0;
    	int maxTries = 3;
    	Scrape.outLog.println("TASKER-      SET TO THROTTLE AT " + Integer.toString(throttleMS) + "EVERY " + Integer.toString(numCycles) + "CYCLE/S");
    	while(true)
    	{
        try {
            URL url = new URL(uri);
            //Parser object
            XmlPullParserFactory factory = XmlPullParserFactory.newInstance();
            factory.setNamespaceAware(false);
            XmlPullParser xpp = factory.newPullParser();
            boolean insideItem = false;
            // We will get the XML from an input stream
            xpp.setInput(getInputStream(url));
            //xpp.setInput(getInputStream(url), "UTF_8");
            int eventType = xpp.getEventType();
            String TAG = "";
            boolean IMAGE = false;
            boolean isImageSet = false;
            boolean finishedimage = false;
            String publisher = "";

            // Runs a for loop FIELDNUM of times, where FIELDNUM is the number of data fields we're going to keep from the parse
            // Initializes the item ArrayList to make each field start as "UNAVAILABLE" which will be overwritten with real data later
            for (int i = 0; i < FIELDNUM; i++) {
                links.add("UNAVAILABLE");
                Scrape.outLog.println("TASKER-      INITIALIZED FIELDS TO 'UNAVAILABLE'");
            }


            // Runs until the end of the read-in document
            while (eventType != XmlPullParser.END_DOCUMENT) {

                if (eventType == XmlPullParser.START_DOCUMENT) {   //START_DOCTUMENT = 0
                    //Log.d("CODE:","Start document");
                } else if (eventType == XmlPullParser.START_TAG) {  //START TAG = 2
                    TAG = xpp.getName();
                    Scrape.outLog.println("TASKER- BEGIN TAG: " + xpp.getName());

                    //Break out of the parse if the XML file contains errors that would break parsing
                    if (TAG.equalsIgnoreCase("Error")) {
                    	Scrape.outLog.println("TASKER-      PAGE CONTAINS ERRORS!");
                        break;
                    }
                    //Log.d("CODE:","Start tag " + TAG);

                    //This flag tells us that we've reached an ImageSet in the xml
                    if (TAG.equalsIgnoreCase("ImageSet")) {
                    	Scrape.outLog.println("TASKER-      ITEM CONTAINS AN IMAGE SET");
                        isImageSet = true;
                    }
                    //This flag tells us the ImageSet has at least 1 Large Image
                    if (isImageSet && TAG.equalsIgnoreCase("LargeImage")) {
                    	Scrape.outLog.println("TASKER-      IMAGESET CONTAINS LARGE IMAGE");
                        IMAGE = true;
                    }
                } else if (eventType == XmlPullParser.END_TAG) {    //END TAG = 3
                    TAG = "";
                    Scrape.outLog.println("TASKER- END TAG: " + xpp.getName());
                    //Log.d("CODE:","End tag " + xpp.getName());
                } else if (eventType == XmlPullParser.TEXT) {
                    //Log.d("CODE:","Text " + xpp.getText());

                    //Sets ASIN to links[0]
                    if (TAG.equalsIgnoreCase("ASIN")) {

                        String TEXT = xpp.getText();
                        Scrape.outLog.println("TASKER-      LOGGED ASIN: " + TEXT);
                        //Log.d("CODE:","Text " + TEXT);
                        links.set(0, TEXT);
                    }
                    else if (TAG.equalsIgnoreCase("SalesRank")) {

                        String TEXT = xpp.getText();
                        Scrape.outLog.println("TASKER-      LOGGED SALESRANK: " + TEXT);
                        //Log.d("CODE:","Text " + TEXT);
                        links.set(5, TEXT);
                    }

                    //Sets the Affiliate Link to links[1]
                    else if (TAG.equalsIgnoreCase("detailpageurl")) {
                        String TEXT = xpp.getText();
                        Scrape.outLog.println("TASKER-      LOGGED AFFILIATE URL: " + TEXT);
                        //Log.d("CODE:","Text " + TEXT);
                        links.set(3, TEXT);
                    }

                    //If we are under the ImageSet tag, and found a Large Image, and do not currently have an image stored, store image url in links[2] and reset flags
                    else if (TAG.equalsIgnoreCase("URL") && IMAGE && !finishedimage) {

                        String TEXT = xpp.getText();
                        Scrape.outLog.println("TASKER-      LOGGED LARGEIMAGE: " + TEXT);
                        //Log.d("CODE:","Text " + TEXT);
                        //headlines.add(TEXT);
                        links.set(4, TEXT);
                        isImageSet = false;
                        IMAGE = false;
                        finishedimage = true;
                    }

                    //Sets the Brand into links[3]
                    else if (TAG.equalsIgnoreCase("Brand")) //TODO: CATEGORIES NEEDED! (NOT EXACT TAG MATCH)
                    {
                        String TEXT = xpp.getText();
                        Scrape.outLog.println("TASKER-      LOGGED BRAND: " + TEXT);
                        //Log.d("CODE:","Text " + TEXT);
                        links.set(2, TEXT);

                    }

                    //If there was no Brand, sets Publisher to links[3]
                    else if (TAG.equalsIgnoreCase("Publisher")) {
                        String TEXT = xpp.getText();
                        Scrape.outLog.println("TASKER-      ITEM MISSING BRAND!");
                        //Log.d("CODE:","Text " + TEXT);
                        //links.add(TEXT);
                        if (links.get(2).equalsIgnoreCase("Unavailable")) {
                            links.set(2, TEXT);
                        }
                        Scrape.outLog.println("TASKER-      LOGGED PUBLISHER: " + TEXT);

                    }

                    //Sets the Title of the item to links[4]
                    else if (TAG.equalsIgnoreCase("Title")) //This is the lowest parsed section on the tree
                    {

                        String TEXT = xpp.getText();
                        //Log.d("CODE:","Text " + TEXT);
                        links.set(1, TEXT);

                        //If the product doesn't have an ASIN, we've reached the end of the product list
                        for (int i = 0; i < FIELDNUM; i++) {
                            if (!links.get(i).equalsIgnoreCase("UNAVAILABLE")) {
                                break;
                            }
                            Scrape.outLog.println("TASKER-      REACHED END OF FILE!");
                            eof = true;
                        }

                        //If we're at the end of the file, stop looping through empty pages. That would cause a crash
                        if (eof)
                        {
                            break;
                        }

                        //Adds the finished item to the array to be exported to SearchActivity
                        headlines.add(links);

//potential throttle point

                        //Throttling Test
                        try
                        {
                            if(shouldThrottle % numCycles == 0)
                            {
                            	Scrape.outLog.println("TASKER- SCRAPE THROTTLED " + Integer.toString(throttleMS) + "ms on CYCLE: " + Integer.toString(shouldThrottle));
                                Thread.sleep(throttleMS);
                            }
                            //Log.d("myTag", "Page Sent To Headlines");

                        }
                        catch (Exception e)
                        {

                        }
                        shouldThrottle++;

                        //Reset the links array to a new array, and leave the hanging reference to the old information so we don't lose what we just pushed
                        //Possible memory leak here
                        links = new ArrayList<String>();

                        //Populates default values into links to be used on the next page of results
                        for (int i = 0; i < FIELDNUM; i++) {
                            links.add("UNAVAILABLE");
                        }
                        publisher = "";

                        //Reset all flags
                        isImageSet = false;
                        IMAGE = false;
                        finishedimage = false;


                    }
                    //end of tags


                }
                //Moves the program onto the next line of XML to read
                eventType = xpp.next();

            }


        } catch (Exception e) {
        	System.out.println(e.getMessage());
        	Scrape.outLog.println("TASKER-      EXCEPTION: " + e.getMessage());
            //e.printStackTrace();
            //Log.d("ERROR PAGE SKIP:",e.getMessage());
        	if(++count == maxTries)
        	{
        		Scrape.outLog.println("TASKER-      FAILED " + Integer.toString(count) + " TIMES... ABORTING PAGE!!!");
        		break;
        	}
        	else
        	{
        		Scrape.outLog.println("TASKER-      FAILED " + Integer.toString(count) + " TIMES... RETRYING PAGE!");
        		continue;
        	}
        }
        break;
    }


        //Puts headlines into the multithreaded params array to be sent back to the main thread for adding to the master list
        //This happens after all pages from the current search have been parsed
    	Scrape.outLog.println("TASKER-      PAGE PASSED INTO MASTER ARRAY!");
        params = headlines;

        //Log.d("myTag", "Headlines Sent To Master");

        //Send the information to the master
        for(int i = 0;i<params.size();i++)
        {
        	System.out.println(params.get(i));
        }
        
        return params;
    }
}
