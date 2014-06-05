package hu.sztaki.stratosphere.workshop.utils;

import java.util.Arrays;

public class Utiltest {
	public static void main(String[] argv) {
		double[][] partialItemFeature = Util.getItemMatrix(10);
		System.out.println(partialItemFeature.length);
		for (int i = 0; i < partialItemFeature.length; i++) {
			System.out.println(Arrays.toString(partialItemFeature[i]));
		}
		System.out.println(Arrays.toString(Util.getItemIDs()));
		partialItemFeature = Util.getItemMatrix(10);
		System.out.println(partialItemFeature.length);
		for (int i = 0; i < partialItemFeature.length; i++) {
			System.out.println(Arrays.toString(partialItemFeature[i]));
		}
		System.out.println(Arrays.toString(Util.getItemIDs()));
		
		System.out.println(Util.getUserMatrix().length);
		for(int i=0;i<10;i++){
			System.out.println(Arrays.toString(Util.getUserMatrix()[i]));
		}
	}
}
