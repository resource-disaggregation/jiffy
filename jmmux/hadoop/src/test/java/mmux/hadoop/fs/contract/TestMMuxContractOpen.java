package mmux.hadoop.fs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractOpenTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

public class TestMMuxContractOpen extends AbstractContractOpenTest {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new MMuxContract(conf);
  }

  @Override
  public void teardown() throws Exception {
    super.teardown();
    ((MMuxContract) getContract()).close();
  }
}
