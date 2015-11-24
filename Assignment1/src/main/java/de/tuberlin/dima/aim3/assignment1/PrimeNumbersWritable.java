package de.tuberlin.dima.aim3.assignment1;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PrimeNumbersWritable implements Writable {

	private int[] numbers;

	public PrimeNumbersWritable() {
		numbers = new int[0];
	}

	public PrimeNumbersWritable(int... numbers) {
		this.numbers = numbers;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		for (int i = 0; i < numbers.length; i++) {
			out.writeInt(numbers[i]);
		}

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		boolean hasMore = true;
		List<Integer>  numberList = new ArrayList<Integer>();
		while (hasMore) {
			try {
				numberList.add(in.readInt());
			} catch (EOFException eof) {
				hasMore = false;
			}
		}
		numbers = new int[numberList.size()];
		for(int i = 0; i<numberList.size();i++){
			numbers[i] = numberList.get(i);
		}
		
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof PrimeNumbersWritable) {
			PrimeNumbersWritable other = (PrimeNumbersWritable) obj;
			return Arrays.equals(numbers, other.numbers);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(numbers);
	}
}