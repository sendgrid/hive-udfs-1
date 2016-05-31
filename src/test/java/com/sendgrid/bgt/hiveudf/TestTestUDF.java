package com.sendgrid.bgt.hiveudf;


import org.junit.Test;

import java.io.IOException;

import org.apache.hadoop.hive.ql.metadata.HiveException;


/**
 * This is a UDF to look a property of an IP address using MaxMind GeoIP2
 * library.
 *
 * The function will need three arguments. <ol> <li>IP Address in string
 * format.</li> <li>IP attribute (e.g. COUNTRY, CITY, REGION, etc)</li>
 * <li>Database file name.</li> </ol>
 *
 * This is a derived version from https://github.com/petrabarus/HiveUDFs.
 * (Please let me know if I need to modify the license)
 *
 * @author Daniel Muller <daniel@spuul.com>
 * @see https://github.com/petrabarus/HiveUDFs
 */
//@UDFType(deterministic = true)
//@Description(
//  name = "testfunction",
//value = "_FUNC_(value) - Doesn't do anything useful.\n"
//+ "Usage: > _FUNC_(ip_string, attribute_name, database_file_name)")
public class TestTestUDF
{
	@Test
	public void test_evaluate() throws HiveException 
	{
		MaxmindGeoIPLookup tester = new MaxmindGeoIPLookup();
		
	
		tester.performLookup(new String("72.130.240.57"), new String("CITY"), 
				new String("/Users/rrapplean/Documents/workspace/GeoIP2-City_1459123200.mmdb"),
				true);
		try {
			tester.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

//	@Test
//    public void getVal() throws IOException, GeoIp2Exception 
//	{
//		TestUDF.getVal("COUNTRY", new CountryResponse());
//    }
//
//    public static String getVal(String dataType, CityResponse response) throws IOException, GeoIp2Exception {
//            if (dataType.equals("COUNTRY_CODE")
//                || dataType.equals("COUNTRY_NAME")
//                || dataType.equals("SUBDIVISION_NAME")
//                || dataType.equals("SUBDIVISION_CODE")
//                || dataType.equals("CITY")
//                || dataType.equals("POSTAL_CODE")
//                || dataType.equals("LONGITUDE")
//                || dataType.equals("LATITUDE")
//            ) {
//                    if (dataType.equals("COUNTRY_CODE") || dataType.equals("COUNTRY_NAME")) {
//                            Country country = response.getCountry();
//                            if (dataType.equals("COUNTRY_CODE")) {
//                                    return country.getIsoCode();
//                            }
//                            else {
//                                    return country.getName();
//                            }
//                    }
//                    if (dataType.equals("SUBDIVISION_CODE") || dataType.equals("SUBDIVISION_NAME")) {
//                            Subdivision subdivision = response.getMostSpecificSubdivision();
//                            if (dataType.equals("SUBDIVISION_CODE")) {
//                                    return subdivision.getIsoCode();
//                            }
//                            else {
//                                    return subdivision.getName();
//                            }
//                    }
//                    if (dataType.equals("CITY")) {
//                            City city = response.getCity();
//                            return city.getName();
//                    }
//                    if (dataType.equals("POSTAL_CODE")) {
//                            Postal postal = response.getPostal();
//                            return postal.getCode();
//                    }
//                    if (dataType.equals("LONGITUDE") || dataType.equals("LATITUDE")) {
//                            Location location = response.getLocation();
//                            if (dataType.equals("LONGITUDE")) {
//                                    return location.getLongitude().toString();
//                            }
//                            else {
//                                    return location.getLatitude().toString();
//                            }
//                    }
//                    return "";
//            }
//            else {
//                    throw new UnsupportedOperationException("Unable get " + dataType + " for Database Type " + response.getClass().getSimpleName());
//            }
//    }
//
//    public static String getVal(String dataType, IspResponse response) throws IOException, GeoIp2Exception {
//            String retVal = "";
//            switch (dataType) {
//                    case "ASN":
//                            retVal = response.getAutonomousSystemNumber().toString();
//                            break;
//                    case "ASN_ORG":
//                            retVal = response.getAutonomousSystemOrganization();
//                            break;
//                    case "ISP":
//                            retVal = response.getIsp();
//                            break;
//                    case "ORG":
//                            retVal = response.getOrganization();
//                            break;
//                    default:
//                            throw new UnsupportedOperationException("Unable get " + dataType + " for Database Type " + response.getClass().getSimpleName());
//            }
//            return retVal;
//    }
//
//    public static String getVal(String dataType, AnonymousIpResponse response) throws IOException, GeoIp2Exception {
//            Boolean retVal = false;
//            switch (dataType) {
//                    case "IS_ANONYMOUS":
//                            retVal = response.isAnonymous();
//                            break;
//                    case "IS_ANONYMOUS_VPN":
//                            retVal = response.isAnonymousVpn();
//                            break;
//                    case "IS_ISP":
//                            retVal = response.isHostingProvider();
//                            break;
//                    case "IS_PUBLIC_PROXY":
//                            retVal = response.isPublicProxy();
//                            break;
//                    case "IS_TOR_EXIT_NODE":
//                            retVal = response.isTorExitNode();
//                            break;
//                    default:
//                            throw new UnsupportedOperationException("Unable get " + dataType + " for Database Type " + response.getClass().getSimpleName());
//            }
//            return retVal ? "true" : "false";
//    }
//
//    public static String getVal(String dataType, DomainResponse response) throws IOException, GeoIp2Exception {
//            if (dataType.equals("DOMAIN")) {
//                    return response.getDomain();
//            }
//            else {
//                    throw new UnsupportedOperationException("Unable get " + dataType + " for Database Type " + response.getClass().getSimpleName());
//            }
//    }
//
//    public static String getVal(String dataType, ConnectionTypeResponse response) throws IOException, GeoIp2Exception {
//            if (dataType.equals("CONNECTION")) {
//                    return response.getConnectionType().toString();
//            }
//            else {
//                    throw new UnsupportedOperationException("Unable get " + dataType + " for Database Type " + response.getClass().getSimpleName());
//            }
//    }
//
//	@Override
//	public String getDisplayString(String[] args) 
//	{
//        if (args.length != 3)
//        	return "_FUNC_( ip_address, attribute_name, database_name )";
//        return "_FUNC_( " + args[0] + ", " + args[1] + ", " + args[2] + " )";
//	}
//
//	@Override
//	public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException 
//	{
//        if (args.length != 3) 
//        {
//            throw new UDFArgumentLengthException("_FUNC_ accepts 3 arguments: "
//            		+ "((Text) ip_address, (Text) Attribute to return, (Text) database_file) " 
//            		+ args.length + " found.");
//        }
//
//		for (int i = 0; i < args.length; i++) 
//		{
//			if (args[i].getCategory() != Category.PRIMITIVE) 
//			{
//				throw new UDFArgumentTypeException(i,
//					"A string argument was expected but an argument of type " 
//					+ args[i].getTypeName() + " was given.");
//	 
//			}
//	 
//			// Now that we have made sure that the argument is of primitive type, we can get the primitive
//			// category
//			PrimitiveCategory primitiveCategory = ((PrimitiveObjectInspector) args[i])
//				.getPrimitiveCategory();
//	 
//			if (primitiveCategory != PrimitiveCategory.STRING
//				&& primitiveCategory != PrimitiveCategory.VOID) 
//			{
//				throw new UDFArgumentTypeException(i,
//				"A string argument was expected but an argument of type " + args[i].getTypeName()
//				+ " was given.");
//	 
//			}
//		}
//
//		converters = new ObjectInspectorConverters.Converter[args.length];
//	 	for (int i = 0; i < args.length; i++) 
//	 	{
//	 		converters[i] = ObjectInspectorConverters.getConverter(args[i],
//	 		PrimitiveObjectInspectorFactory.writableStringObjectInspector);
//	 	}
//	 
//		// We will be returning a Text object
//	 	return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
//	}
}
