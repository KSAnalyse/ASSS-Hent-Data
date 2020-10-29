using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.Net;
using System.IO;
using System.Threading;

public class SSBJsonQuery
{
    public string Id { get; set; }
    public string Path { get; set; }
    public string Title { get; set; }
    public double Score { get; set; }
    public DateTime Published { get; set; }
}

public class Program
{
	public static void Main()
	{
		int[] table = {12115, 12189, 12238, 12275,12276,12279,12280,12286,12287,12305,12615,12150,12151,12612,12292,12003,12293,11924,11875,11933,11820,11805,11814,11816,11845,12183,12367,12295,05939,11211,12303,12920,12611,11879,12676,12597,12364,11994,11996,12005,11993,11995,11906,12559,12905,13006,12160,12213,12203,12222,12272,12056,12129,12562,12216,12436,09345,11977,12285,11974,12236,12234,12282,11971,12247,12860,12861,08655,12055,11975,12919,12209,13013};
		
		foreach (int Tabellnummer in table)
		{
			string url = "http://data.ssb.no/api/v0/no/table/?query=title:" + Tabellnummer;
        	HttpWebRequest request = WebRequest.Create(url) as HttpWebRequest;
        	request.Method = "GET";
        	var jsonValue = "";

        	using (HttpWebResponse response = request.GetResponse() as HttpWebResponse)
        	{
            	StreamReader reader = new StreamReader(response.GetResponseStream());
            	jsonValue = reader.ReadToEnd();
        	}
        	IList<SSBJsonQuery> query = JsonConvert.DeserializeObject<IList<SSBJsonQuery>>(jsonValue);
			Thread.Sleep(1000);
			Console.WriteLine(query[0].Published + " " + Tabellnummer);
		}		
	}
}