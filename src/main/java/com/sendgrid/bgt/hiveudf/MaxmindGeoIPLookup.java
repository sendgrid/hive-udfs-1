package com.sendgrid.bgt.hiveudf;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.AnonymousIpResponse;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.ConnectionTypeResponse;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.model.DomainResponse;
import com.maxmind.geoip2.model.IspResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Location;
import com.maxmind.geoip2.record.Postal;
import com.maxmind.geoip2.record.Subdivision;

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
public class MaxmindGeoIPLookup extends GenericUDF
{
	private Converter[] converters;

	@Override
	public Object evaluate(DeferredObject[] args) throws HiveException
	{

		if (args == null || args.length < 3
				|| args[0].get() == null
				|| args[1].get() == null
				|| args[2].get() == null)
			return null;
		boolean debug = false;
		String ip = ((Text) converters[0].convert(args[0].get())).toString();
		String attributeName = ((Text) converters[1].convert(args[1].get())).toString();
		String databaseName = ((Text) converters[2].convert(args[2].get())).toString();
		if (args.length == 4)
			debug = true;
			
		return performLookup(ip, attributeName, databaseName, debug);
	}

	@SuppressWarnings("unused")
	private Text errorVal(boolean debug, String ifDebug, String ifNotDebug)
	{
		if (debug)
			return new Text(ifDebug);
		else
			return new Text(ifNotDebug);
	}

	private Text errorVal(boolean debug, String ifDebug)
	{
		if (debug)
			return new Text(ifDebug);
		else
			return new Text("");
	}

	public Object performLookup(String iIP, String iAttributeName, String iDatabaseName, boolean debug)
	{

		File database = new File(iDatabaseName);
		if (iIP == null)
		{
			return errorVal(debug, "iIP is null");
		}
		if (iAttributeName == null)
		{
			return errorVal(debug, "iAttributeName is null");
		}
		if (iDatabaseName == null)
		{
			return errorVal(debug, "iDatabaseName is null");
		}

		DatabaseReader reader;
		String databaseType;
		InetAddress ipAddress;

		try
		{
			// This creates the DatabaseReader object, which should be reused across
			// lookups.
			reader = new DatabaseReader.Builder(database).build();
			if (reader == null)
			{
				return errorVal(debug, "reader is null");
			}
		}
		catch(Exception e) {
			return errorVal(debug, "build: " + e.getStackTrace());
		}
		try
		{
			databaseType = reader.getMetadata().getDatabaseType();
			if (databaseType == null)
			{
				return errorVal(debug, "databaseType is null");
			}
		}
		catch(Exception e) {
			return errorVal(debug, "getDatabaseType Error: " + e.getStackTrace());
		}
		try
		{
			ipAddress = InetAddress.getByName(iIP);
			if (ipAddress == null)
			{
				return errorVal(debug, "ipAddress is null");
			}
		}
		catch(Exception e) {
			return errorVal(debug, "getByName Error: " + e.getStackTrace());
		}
		try
		{
			String retVal = new String ("Lookup failure");
			switch (databaseType) {
					case "GeoIP2-Country":
					case "GeoLite2-Country":
							retVal = getVal(iAttributeName, reader.country(ipAddress));
							break;
					case "GeoIP2-City":
					case "GeoLite2-City":
							retVal = getVal(iAttributeName, reader.city(ipAddress));
							break;
					case "GeoIP2-Anonymous-IP":
							retVal = getVal(iAttributeName, reader.anonymousIp(ipAddress));
							break;
					case "GeoIP2-Connection-Type":
							retVal = getVal(iAttributeName, reader.connectionType(ipAddress));
							break;
					case "GeoIP2-Domain":
							retVal = getVal(iAttributeName, reader.domain(ipAddress));
							break;
					case "GeoIP2-ISP":
							retVal = getVal(iAttributeName, reader.isp(ipAddress));
							break;
					default:
							return errorVal(debug, "Unknown database type " + databaseType);
			}
			return new Text(retVal);
		}
		catch (AddressNotFoundException anfe)
		{
			return errorVal(debug, "AddressNotFoundException: " + anfe.getMessage());
		}
		catch(Exception e) 
		{
			return errorVal(debug, "Unrecognized Exception(" + e.getClass().getName() 
						+ ") message:" + e.getMessage());
		}
	}

	public static String getVal(String dataType, CountryResponse response) throws IOException, GeoIp2Exception {
			if (dataType.equals("COUNTRY_CODE") || dataType.equals("COUNTRY_NAME")) {
					Country country = response.getCountry();
					if (dataType.equals("COUNTRY_CODE")) {
							return country.getIsoCode();
					}
					else {
							return country.getName();
					}
			}
			else {
					throw new UnsupportedOperationException("Unable get " + dataType + " for Database Type " + response.getClass().getSimpleName());
			}
	}

	public static String getVal(String dataType, CityResponse response) throws IOException, GeoIp2Exception {
			if (dataType.equals("COUNTRY_CODE")
				|| dataType.equals("COUNTRY_NAME")
				|| dataType.equals("SUBDIVISION_NAME")
				|| dataType.equals("SUBDIVISION_CODE")
				|| dataType.equals("CITY")
				|| dataType.equals("POSTAL_CODE")
				|| dataType.equals("LONGITUDE")
				|| dataType.equals("LATITUDE")
			) {
					if (dataType.equals("COUNTRY_CODE") || dataType.equals("COUNTRY_NAME")) {
							Country country = response.getCountry();
							if (dataType.equals("COUNTRY_CODE")) {
									return country.getIsoCode();
							}
							else {
									return country.getName();
							}
					}
					if (dataType.equals("SUBDIVISION_CODE") || dataType.equals("SUBDIVISION_NAME")) {
							Subdivision subdivision = response.getMostSpecificSubdivision();
							if (dataType.equals("SUBDIVISION_CODE")) {
									return subdivision.getIsoCode();
							}
							else {
									return subdivision.getName();
							}
					}
					if (dataType.equals("CITY")) {
							City city = response.getCity();
							return city.getName();
					}
					if (dataType.equals("POSTAL_CODE")) {
							Postal postal = response.getPostal();
							return postal.getCode();
					}
					if (dataType.equals("LONGITUDE") || dataType.equals("LATITUDE")) {
							Location location = response.getLocation();
							if (dataType.equals("LONGITUDE")) {
									return location.getLongitude().toString();
							}
							else {
									return location.getLatitude().toString();
							}
					}
					return "";
			}
			else {
					throw new UnsupportedOperationException("Unable get " + dataType + " for Database Type " + response.getClass().getSimpleName());
			}
	}

	public static String getVal(String dataType, IspResponse response) throws IOException, GeoIp2Exception {
			String retVal = "";
			switch (dataType) {
					case "ASN":
							retVal = response.getAutonomousSystemNumber().toString();
							break;
					case "ASN_ORG":
							retVal = response.getAutonomousSystemOrganization();
							break;
					case "ISP":
							retVal = response.getIsp();
							break;
					case "ORG":
							retVal = response.getOrganization();
							break;
					default:
							throw new UnsupportedOperationException("Unable get " + dataType + " for Database Type " + response.getClass().getSimpleName());
			}
			return retVal;
	}

	public static String getVal(String dataType, AnonymousIpResponse response) throws IOException, GeoIp2Exception {
			Boolean retVal = false;
			switch (dataType) {
					case "IS_ANONYMOUS":
							retVal = response.isAnonymous();
							break;
					case "IS_ANONYMOUS_VPN":
							retVal = response.isAnonymousVpn();
							break;
					case "IS_ISP":
							retVal = response.isHostingProvider();
							break;
					case "IS_PUBLIC_PROXY":
							retVal = response.isPublicProxy();
							break;
					case "IS_TOR_EXIT_NODE":
							retVal = response.isTorExitNode();
							break;
					default:
							throw new UnsupportedOperationException("Unable get " + dataType + " for Database Type " + response.getClass().getSimpleName());
			}
			return retVal ? "true" : "false";
	}

	public static String getVal(String dataType, DomainResponse response) throws IOException, GeoIp2Exception {
			if (dataType.equals("DOMAIN")) {
					return response.getDomain();
			}
			else {
					throw new UnsupportedOperationException("Unable get " + dataType + " for Database Type " + response.getClass().getSimpleName());
			}
	}

	public static String getVal(String dataType, ConnectionTypeResponse response) throws IOException, GeoIp2Exception {
			if (dataType.equals("CONNECTION")) {
					return response.getConnectionType().toString();
			}
			else {
					throw new UnsupportedOperationException("Unable get " + dataType + " for Database Type " + response.getClass().getSimpleName());
			}
	}

	@Override
	public String getDisplayString(String[] args)
	{
		if (args.length != 3)
			return "_FUNC_( ip_address, attribute_name, database_name )";
		return "_FUNC_( " + args[0] + ", " + args[1] + ", " + args[2] + " )";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException
	{
		if (args.length < 3 || args.length > 4)
		{
			throw new UDFArgumentLengthException("MaxmindGeoIPLookup accepts 3 or 4 arguments: "
					+ "((Text) ip_address, (Text) Attribute to return, (Text) database_file) "
					+ args.length + " found.");
		}

		for (int i = 0; i < args.length; i++)
		{
			if (args[i].getCategory() != Category.PRIMITIVE)
			{
				throw new UDFArgumentTypeException(i,
					"A string argument was expected but an argument of type "
					+ args[i].getTypeName() + " was given.");

			}

			// Now that we have made sure that the argument is of primitive type, we can get the primitive
			// category
			PrimitiveCategory primitiveCategory = ((PrimitiveObjectInspector) args[i])
				.getPrimitiveCategory();

			if (primitiveCategory != PrimitiveCategory.STRING
				&& primitiveCategory != PrimitiveCategory.VOID)
			{
				throw new UDFArgumentTypeException(i,
				"A string argument was expected but an argument of type " + args[i].getTypeName()
				+ " was given.");

			}
		}

		converters = new ObjectInspectorConverters.Converter[args.length];
		for (int i = 0; i < args.length; i++)
		{
			converters[i] = ObjectInspectorConverters.getConverter(args[i],
					PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		}

		// We will be returning a Text object
		return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
	}
}
