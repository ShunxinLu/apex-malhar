package org.apache.apex.malhar.stream.sample.complete;

import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.Duration;

import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.WindowOption;

import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.CompositeStreamTransform;
import org.apache.apex.malhar.stream.api.WindowedStream;
import org.apache.apex.malhar.stream.api.function.Function;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;

import com.google.common.collect.Sets;

import com.datatorrent.contrib.twitter.TwitterSampleInput;
import com.datatorrent.lib.util.KeyValPair;

/**
 * Created by Shunxin on 7/12/16.
 */

public class TwitterAutoComplete
{
  public static class StringUtils
  {
    static CharsetEncoder encoder = Charset.forName("US-ASCII").newEncoder();

    public static boolean isAscii(String v)
    {
      return encoder.canEncode(v);
    }
  }

  private static class ExtractHashtags implements Function.FlatMapFunction<String, String>
  {

    @Override
    public Iterable<String> f(String input)
    {
      List<String> result = new LinkedList<>();
      Matcher m = Pattern.compile("#\\S+").matcher(input);
      while (m.find()) {
        result.add(m.group().substring(1));
      }
      return result;
    }
  }

  /**
   * Class used to store tag-count pairs.
   */
  public static class CompletionCandidate implements Comparable<TwitterAutoComplete.CompletionCandidate>
  {
    private long count;
    private String value;

    public CompletionCandidate(String value, long count)
    {
      this.value = value;
      this.count = count;
    }

    public long getCount()
    {
      return count;
    }

    public String getValue()
    {
      return value;
    }

    // Empty constructor required for Avro decoding.
    public CompletionCandidate() {}

    @Override
    public int compareTo(TwitterAutoComplete.CompletionCandidate o)
    {
      if (this.count < o.count) {
        return -1;
      } else if (this.count == o.count) {
        return this.value.compareTo(o.value);
      } else {
        return 1;
      }
    }

    @Override
    public boolean equals(Object other)
    {
      if (other instanceof TwitterAutoComplete.CompletionCandidate) {
        TwitterAutoComplete.CompletionCandidate that = (TwitterAutoComplete.CompletionCandidate)other;
        return this.count == that.count && this.value.equals(that.value);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode()
    {
      return Long.valueOf(count).hashCode() ^ value.hashCode();
    }

    @Override
    public String toString()
    {
      return "CompletionCandidate[" + value + ", " + count + "]";
    }
  }

  /**
   * Lower latency, but more expensive.
   */
  private static class ComputeTopFlat
    extends CompositeStreamTransform<TwitterAutoComplete.CompletionCandidate, Tuple.WindowedTuple<KeyValPair<String, List<TwitterAutoComplete.CompletionCandidate>>>>
  {
    private final int candidatesPerPrefix;
    private final int minPrefix;

    public ComputeTopFlat(int candidatesPerPrefix, int minPrefix)
    {
      this.candidatesPerPrefix = candidatesPerPrefix;
      this.minPrefix = minPrefix;
    }

    @Override
    public ApexStream<Tuple.WindowedTuple<KeyValPair<String, List<TwitterAutoComplete.CompletionCandidate>>>> compose(
      ApexStream<TwitterAutoComplete.CompletionCandidate> input)
    {
      return ((WindowedStream<KeyValPair<String,CompletionCandidate>>)input.flatMap(new TwitterAutoComplete.AllPrefixes(minPrefix)))
        .topByKey(candidatesPerPrefix, new Function.MapFunction<KeyValPair<String, TwitterAutoComplete.CompletionCandidate>, Tuple<KeyValPair<String,
          TwitterAutoComplete.CompletionCandidate>>>()
        {
          @Override
          public Tuple<KeyValPair<String, TwitterAutoComplete.CompletionCandidate>> f(KeyValPair<String, TwitterAutoComplete.CompletionCandidate> tuple)
          {
            return new Tuple.PlainTuple<>(tuple);
          }
        });
    }
  }


  private static class AllPrefixes implements Function.FlatMapFunction<TwitterAutoComplete.CompletionCandidate, KeyValPair<String, TwitterAutoComplete.CompletionCandidate>>
  {
    private final int minPrefix;
    private final int maxPrefix;

    public AllPrefixes()
    {
      this(0, Integer.MAX_VALUE);
    }

    public AllPrefixes(int minPrefix)
    {
      this(minPrefix, Integer.MAX_VALUE);
    }

    public AllPrefixes(int minPrefix, int maxPrefix)
    {
      this.minPrefix = minPrefix;
      this.maxPrefix = maxPrefix;
    }

    @Override
    public Iterable<KeyValPair<String, TwitterAutoComplete.CompletionCandidate>> f(TwitterAutoComplete.CompletionCandidate input)
    {
      List<KeyValPair<String, TwitterAutoComplete.CompletionCandidate>> result = new LinkedList<>();
      String word = input.getValue();
      for (int i = minPrefix; i <= Math.min(word.length(), maxPrefix); i++) {
        result.add(new KeyValPair<>(input.getValue().substring(0, i).toLowerCase(), input));
      }
      return result;
    }
  }

  /**
   * A Composite stream transform that takes as input a list of tokens and returns
   * the most common tokens per prefix.
   */
  public static class ComputeTopCompletions
    extends CompositeStreamTransform<String, Tuple.WindowedTuple<KeyValPair<String, List<TwitterAutoComplete.CompletionCandidate>>>>
  {
    private final int candidatesPerPrefix;
    private final boolean recursive;

    protected ComputeTopCompletions(int candidatesPerPrefix, boolean recursive)
    {
      this.candidatesPerPrefix = candidatesPerPrefix;
      this.recursive = recursive;
    }

    public static TwitterAutoComplete.ComputeTopCompletions top(int candidatesPerPrefix, boolean recursive)
    {
      return new TwitterAutoComplete.ComputeTopCompletions(candidatesPerPrefix, recursive);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ApexStream<Tuple.WindowedTuple<KeyValPair<String, List<TwitterAutoComplete.CompletionCandidate>>>> compose(ApexStream<String> inputStream)
    {
      if (!(inputStream instanceof WindowedStream)) {
        return null;
      }

      ApexStream<TwitterAutoComplete.CompletionCandidate> candidates = ((WindowedStream<String>)inputStream)
        .countByKey(new Function.MapFunction<String, Tuple<KeyValPair<String, Long>>>()
        {
          @Override
          public Tuple<KeyValPair<String, Long>> f(String input)
          {
            return new Tuple.PlainTuple<>(new KeyValPair<>(input, 1L));
          }
        }).map(new Function.MapFunction<Tuple.WindowedTuple<KeyValPair<String,Long>>, TwitterAutoComplete.CompletionCandidate>()
        {
          @Override
          public TwitterAutoComplete.CompletionCandidate f(Tuple.WindowedTuple<KeyValPair<String, Long>> input)
          {
            return new TwitterAutoComplete.CompletionCandidate(input.getValue().getKey(), input.getValue().getValue());
          }
        });

      return candidates.addCompositeStreams(new TwitterAutoComplete.ComputeTopFlat(candidatesPerPrefix, 1));

    }
  }





  static class SomeFilter implements Function.FilterFunction<String>
  {
    @Override
    public Boolean f(String input)
    {
      return TwitterAutoComplete.StringUtils.isAscii(input);
    }
  }

  public static void main(String[] args)
  {
    boolean stream = Sets.newHashSet(args).contains("--streaming");
    TwitterSampleInput input = new TwitterSampleInput();

    WindowOption windowOption = stream
      ? new WindowOption.TimeWindows(Duration.standardMinutes(30)).slideBy(Duration.standardSeconds(5))
      : new WindowOption.GlobalWindow();

    ApexStream<String> tags = StreamFactory.fromInput(input, input.text)
      .filter(new SomeFilter())
      .flatMap(new TwitterAutoComplete.ExtractHashtags());
    //tags.print();
    tags.window(windowOption, new TriggerOption().accumulatingFiredPanes().withEarlyFiringsAtEvery(Duration.standardSeconds(10)))
      .addCompositeStreams(TwitterAutoComplete.ComputeTopCompletions.top(10, true)).print()
      .runEmbedded(false, 100000, new Callable<Boolean>()
      {
        @Override
        public Boolean call() throws Exception
        {
          return false;
        }
      });
  }
}
