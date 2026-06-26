const multi = "ICHIBI_LAKE_035a2116-c9d0-4603-9f12-da1fb57294d1,ICHIBI_LAKE_RESEARCH_035a2116-c9d0-4603-9f12-da1fb57294d1,ICHIBI_LAKE_TEST_7a96c3e2-b1fe-4705-9064-fc1882674b13";
const validKeys = new Set(multi.split(',').map(k => k.trim()).filter(Boolean));

console.log("Size:", validKeys.size);
console.log("Has Key 1:", validKeys.has("ICHIBI_LAKE_035a2116-c9d0-4603-9f12-da1fb57294d1"));
console.log("Has Key 2:", validKeys.has("ICHIBI_LAKE_RESEARCH_035a2116-c9d0-4603-9f12-da1fb57294d1"));
console.log("Has Key 3:", validKeys.has("ICHIBI_LAKE_TEST_7a96c3e2-b1fe-4705-9064-fc1882674b13"));
console.log("Has Non-existent:", validKeys.has("WRONG_KEY"));
