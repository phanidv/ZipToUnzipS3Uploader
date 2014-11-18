package edu.neu.data.unzipfiles;

import java.util.List;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class UnZipRunner {

	public static void main(String[] args) {

		if (args.length != 2) {
			System.err.println("Usage: unzip <in> <out>");
			System.exit(2);
		}

		String zipFileRoot = args[0];
		String outputFolderRoot = args[1];

		AmazonS3Client amazonS3Client = new AmazonS3Client(
				new BasicAWSCredentials("AKIAI5EVC2RUFYWHJCGA",
						"iiX49qVrgAOM/9X5mYrmMjxLRQC1Nkxy1+cCwCAN"));

		ObjectListing filesName = amazonS3Client.listObjects(zipFileRoot);

		List<S3ObjectSummary> list = filesName.getObjectSummaries();
		for (S3ObjectSummary fi : list) {
			S3Object obj = amazonS3Client.getObject(zipFileRoot, fi.getKey());
			if (obj.getKey().endsWith(".zip")) {
				// System.out.println(obj.getBucketName() + "/" + obj.getKey());
				// }

				/*
				 * List<File> files = (List<File>) FileUtils.listFiles(new File(
				 * zipFileRoot), TrueFileFilter.INSTANCE,
				 * TrueFileFilter.INSTANCE);
				 */
				System.out.println(obj.getKey());
				/*UnZipFile unZipFile = new UnZipFile();

				// for (File file : files) {
				// unZipFile.unZipIt(file.getAbsolutePath(), outputFolderRoot);
				unZipFile.unZipIt(
						"s3://" + obj.getBucketName() + "/" + obj.getKey(),
						outputFolderRoot);*/
			}
		}
	}
}