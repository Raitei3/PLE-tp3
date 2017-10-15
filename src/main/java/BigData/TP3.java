package BigData;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringJoiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// zw,zikamanas village,Zikamanas Village,00,,-18.2166667,27.95

public class TP3 {

public enum WCP{
		nb_cities,
		nb_pop,
		total_pop
	};

  public static class TP3Mapper
       extends Mapper<Object, Text, Text, Data>{

	  		public void map(Object key, Text value, Context context
			  ) throws IOException, InterruptedException {
	  			context.getCounter(WCP.nb_cities).increment(1);
	  			if(!value.toString().matches(".*,.*,.*,.*,,.*")) {
	  				//String val = value.toString();
	  				/*Extraire la pop pour incrémenter total_pop */
	  				context.getCounter(WCP.nb_pop).increment(1);
	  				String[] parts = value.toString().split(",");
	  				if (parts.length == 7){
							try{
								int nbPop=Integer.parseInt(parts[4]);
	  					     context.getCounter(WCP.total_pop).increment(nbPop);
	  				     context.write(new Text(parts[0]), new Data(value, 1, nbPop));
					            }catch (NumberFormatException e) {}
					                 }
	  			  }
	  		}
  }
  public static class TP3Reducer
       extends Reducer<Text,Data,NullWritable,Text> {
	  private MultipleOutputs<NullWritable, Text> multOut;
		public void setup(Context context) {
			this.multOut = new MultipleOutputs<NullWritable, Text>(context);
		}
		
		
    public void reduce(Text key, Iterable<Data> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	
		StringJoiner sj = new StringJoiner("\n");
		int cities = 0;
		int pop = 0;
		for (Data value : values) {
			sj.add(value.entries.toString());
			cities++;
			pop += value.pop.get();
		}
		
		Text entries = new Text(sj.toString());
		Text entry = new Text(String.format("%s,%d,%d", key.toString(), cities, pop));
		
		this.multOut.write("entries", NullWritable.get(), entries);
		this.multOut.write("countries", NullWritable.get(), entry);
    }
  }
  
  

  
  public static class  Data implements Writable {
		public Text entries;
		public IntWritable cities;
		public IntWritable pop;
		
		public Data() {
			this.entries = new Text();
			this.cities = new IntWritable();
			this.pop = new IntWritable();
		}
		
		public Data(Text entries, int cities, int pop) {
			this.entries = entries;
			this.cities = new IntWritable(cities);
			this.pop = new IntWritable(pop);
		}

		public void readFields(DataInput arg0) throws IOException {
			// TODO Auto-generated method stub
			
		}

		public void write(DataOutput arg0) throws IOException {
			// TODO Auto-generated method stub
			
		}
  }
  
  
    public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    System.out.println("coucou");
    Job job = Job.getInstance(conf, "TP3");
    job.setNumReduceTasks(1);
    job.setJarByClass(TP3.class);
    job.setMapperClass(TP3Mapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Data.class);
    job.setReducerClass(TP3Reducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    MultipleOutputs.addNamedOutput(job, "entries", TextOutputFormat.class, NullWritable.class, Text.class);
    MultipleOutputs.addNamedOutput(job, "countries", TextOutputFormat.class, NullWritable.class, Text.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    Counters count = job.getCounters();
    Counter nb_pop = count.findCounter(WCP.nb_pop);
    Counter nb_cities = count.findCounter(WCP.nb_cities);
    Counter total_pop = count.findCounter(WCP.total_pop);
    System.out.println(total_pop.getDisplayName() + ":" + total_pop.getValue());
    System.out.println(nb_pop.getDisplayName() + ":" + nb_pop.getValue());
    System.out.println(nb_cities.getDisplayName() + ":" + nb_cities.getValue());
  }
  
  

}

