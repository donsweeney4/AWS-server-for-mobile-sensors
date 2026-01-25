// test_get_url.mjs
import { getPresignedS3Url } from './s3_utils.js';

(async () => {
  try {
    const result = await getPresignedS3Url("tests3-001.csv");
    console.log("✅ Success:", result);
  } catch (err) {
    console.error("❌ Test failed:", err.message);
  }
})();
