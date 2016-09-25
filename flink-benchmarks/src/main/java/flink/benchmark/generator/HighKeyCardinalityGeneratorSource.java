package flink.benchmark.generator;

import flink.benchmark.BenchmarkConfig;

import org.apache.flink.api.java.tuple.Tuple7;

import java.util.Random;
import java.util.UUID;

/**
 * A data generator for generating large numbers of campaigns
 */
//public class HighKeyCardinalityGeneratorSource extends LoadGeneratorSource<String> {
public class HighKeyCardinalityGeneratorSource extends LoadGeneratorSource<Tuple7<String, String, String, String, String, String, String>> {

  private static final String[] eventTypes = {"view", "click", "purchase"};

  private int eventsIdx = 0;
  //final StringBuilder elementBase = elementBase();
  //final int resetSize = elementBase.length();
  final String user_id = UUID.randomUUID().toString();
  final String page_id = UUID.randomUUID().toString();
  final long campaignMsb = UUID.randomUUID().getMostSignificantBits();
  final long campaignLsbTemplate = UUID.randomUUID().getLeastSignificantBits() & 0xffffffff00000000L;
  final Random random = new Random();
  private int cntCampaigns = 0;

  private final int numCampaigns;

  public HighKeyCardinalityGeneratorSource(BenchmarkConfig config) {
    super(config.loadTargetHz, config.timeSliceLengthMs);
    //System.out.println(config.loadTargetHz);
    //System.out.println(config.timeSliceLengthMs);
    //System.out.println(config.numCampaigns);
    this.numCampaigns = config.numCampaigns;
  }

  @Override
  public Tuple7<String, String, String, String, String, String, String> generateElement() {
  //public String generateElement() {
    if (eventsIdx == eventTypes.length) {
      eventsIdx = 0;
    }

    //long lsb = campaignLsbTemplate + random.nextInt(numCampaigns);
    long lsb = campaignLsbTemplate + cntCampaigns;
    cntCampaigns = (cntCampaigns == numCampaigns - 1) ? 0 : cntCampaigns + 1;
    UUID campaign = new UUID(campaignMsb, lsb);

    Tuple7<String, String, String, String, String, String, String> tuple;
    tuple = new Tuple7<String, String, String, String, String, String, String>(user_id, page_id, campaign.toString(), "banner78", eventTypes[eventsIdx++], String.valueOf(System.currentTimeMillis()), "1.2.3.4");

    //elementBase.setLength(resetSize);
    //elementBase.append(campaign.toString());
    //elementBase.append("\",\"ad_type\":\"banner78\",\"event_type\":\"");
    //elementBase.append(eventTypes[eventsIdx++]);
    //elementBase.append("\",\"event_time\":\"");
    //elementBase.append(System.currentTimeMillis());
    //elementBase.append("\",\"ip_address\":\"1.2.3.4\"}");
    //System.out.println(eventsIdx);

    //return elementBase.toString();
    //return tuple.toString();
    return tuple;
  }

  //private StringBuilder elementBase() {
  //  return new StringBuilder("{\"user_id\":\"" + UUID.randomUUID() + "\",\"page_id\":\"" + UUID.randomUUID() + "\",\"campaign_id\":\"");
  //}
}
