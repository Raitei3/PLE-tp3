package BigData;
import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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
         extends Mapper<Object, Text, NullWritable, Text>{

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
  	  					     context.getCounter(WCP.total_pop).increment(Integer.parseInt(parts[4]));
  	  				       context.write(NullWritable.get(), value);
  					            }catch (NumberFormatException e) {}
  					                 }
  	  			  }
  	  		}
    }
  public static class TP3Reducer
       extends Reducer<NullWritable,Text,NullWritable,Text> {
    public void reduce(NullWritable key, Text values,
                       Context context
                       ) throws IOException, InterruptedException {
      context.write(key, values);
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    System.out.println("coucou");
    Job job = Job.getInstance(conf, "TP3");
    job.setNumReduceTasks(1);
    job.setJarByClass(TP3.class);
    job.setMapperClass(TP3Mapper.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(TP3Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
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
