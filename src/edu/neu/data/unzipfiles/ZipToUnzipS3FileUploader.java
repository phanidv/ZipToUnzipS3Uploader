package edu.neu.data.unzipfiles;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * 
 * Utility class to load unzipped files from the zip files present in a S3 bucket in AWS
 * 
 * Unzips the zip files from the given bucket [bucket_name] and then loads the unzipped files onto the newly 
 * created bucket "[bucket_name].unzipped" in the same folder structure 
 * 
 * @author phanidv88 (phanidv88@gmail.com)
 * @author paraschauhan26 (paraschauhan26@gmail.com)
 * 
 */
public class ZipToUnzipS3FileUploader {

	private static AmazonS3Client amazonS3Client;
	private static File tempFolder;
	private static String destBucket;
	
	public static void main(String[] args) throws IOException {
		
		if (args.length != 3) {
			
			System.err.println("Usage: ZipToUnzipS3FileUploader <access_key> <secret_key> <bucket_name>");
			System.exit(-1);
		}

		//creates a client with the given access_key and the secret_key
		amazonS3Client = new AmazonS3Client(new BasicAWSCredentials(args[0], args[1]));
		
		//creates a temp folder
		tempFolder = new File("/tmp/temp-" + String.valueOf(System.currentTimeMillis()));
		if (!tempFolder.mkdir()) {
			
			System.out.println("Unable to create a temp folder..");
			System.exit(-1);
		}
		
		String bucketName = args[2];
		String srcBucketLocation = null;
		
		try {
			
			srcBucketLocation = amazonS3Client.getBucketLocation(bucketName);
			
		} catch (AmazonS3Exception e) {
			
			//the bucket provided is an invalid one
			System.out.println(e.getMessage());
			System.exit(-1);
		}
		
		//creating the destination bucket
		destBucket = bucketName + ".unzipped";
		amazonS3Client.createBucket(destBucket, srcBucketLocation);
		
		startUnzipProcess(bucketName);
		
		//delete the temp folder created
		tempFolder.delete();
	}

	/**
	 * The main processing method.
	 * 
	 * Retrieves the zip files present in the given bucket
	 * Unzips each file in the tempFolder created and then uploads the unzipped files onto the newly created unzipped bucket
	 * 
	 * @param bucketName
	 * @throws IOException
	 */
	public static void startUnzipProcess(String bucketName) throws IOException {

		System.out.println("Retrieving the zip files..");

		//gets the list of files present in the bucket
		ObjectListing fileNames = amazonS3Client.listObjects(bucketName);

		String zipFileName = null, folderName = null;

		List<S3ObjectSummary> fileList = fileNames.getObjectSummaries();
		for (S3ObjectSummary file : fileList) {

			//process only the files having the zip extension
			S3Object obj = amazonS3Client.getObject(bucketName, file.getKey());
			String objKey = obj.getKey();
			if (objKey.endsWith(".zip")) {

				//extracting the zip filename from object key
				zipFileName = objKey.substring(objKey.lastIndexOf("/") + 1);
				
				//extracts the folder location of the zip file
				folderName = objKey.substring(0, objKey.lastIndexOf("/") + 1);

				//unzips the zip file into a temp folder
				unZipIt(zipFileName, obj.getObjectContent(), tempFolder);

				//places the unzipped files onto the unzipped bucket on s3 
				putUnzipFilesOnS3(folderName, tempFolder);

				//"prepares" the temp folder for the next zip file
				FileUtils.cleanDirectory(tempFolder); 
			}
			
			obj.close();
		}
	}
	
	/**
	 * Unzips the given file and then places the contents in the tempFolder
	 * 
	 * @throws IOException 
	 */
	public static void unZipIt(String zipFile, InputStream zipInputStream, File tempFolder) throws IOException {

		byte[] buffer = new byte[1024];

		System.out.println("Starting Unzipping " + zipFile);

		//gets the zip file content
		ZipInputStream zis = new ZipInputStream(zipInputStream);
		//gets the zipped file list entry
		ZipEntry ze = zis.getNextEntry();

		int count = 0;

		while (ze != null) {
			count++;

			String fileName = ze.getName();
			File newFile = new File(tempFolder + File.separator	+ fileName);

			//create all non exists folders
			//else you will hit FileNotFoundException for compressed folder
			new File(newFile.getParent()).mkdirs();

			FileOutputStream fos = new FileOutputStream(newFile);

			int len;
			while ((len = zis.read(buffer)) > 0) {
				fos.write(buffer, 0, len);
			}

			fos.close();
			ze = zis.getNextEntry();
		}

		zis.closeEntry();
		zis.close();

		System.out.println("Unzipped " + count + " files. Source zip: " + zipFile);
	}
	
	/**
	 * Places the extracted unzip files into the newly created S3 bucket
	 * 
	 * @param folderName
	 * @param tempFolder
	 */
	public static void putUnzipFilesOnS3(String folderName, File tempFolder) {

		System.out.println("Uploading files to S3..");

		@SuppressWarnings("unchecked")
		List<File> files = (List<File>) FileUtils.listFiles(tempFolder, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
		for (File file : files) {

			amazonS3Client.putObject(new PutObjectRequest(destBucket, folderName + file.getName(), file));
		} 

		System.out.println("Done..");	
	}
}
