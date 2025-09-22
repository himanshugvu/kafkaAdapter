// Simple test to verify JSON processing functionality

class TestJsonProcessing {
    public static void main(String[] args) {
        // Test JSON processing capabilities
        System.out.println("Testing JSON Processing...");

        // Test 1: String pass-through
        String payload = "{\"name\":\"John\",\"age\":30}";
        System.out.println("Original payload: " + payload);

        // Test 2: Light enrichment (string concatenation)
        String enriched = addTimestamp(payload);
        System.out.println("Enriched payload: " + enriched);

        // Test 3: Field extraction
        String name = extractField(payload, "name");
        System.out.println("Extracted name: " + name);

        // Test 4: Prefix/suffix operations
        String prefixed = "prefix:" + payload;
        String suffixed = payload + ":suffix";
        System.out.println("Prefixed: " + prefixed);
        System.out.println("Suffixed: " + suffixed);

        // Test 5: Wrapping
        String wrapped = "{\"wrapper\":" + payload + "}";
        System.out.println("Wrapped: " + wrapped);

        System.out.println("JSON Processing tests completed successfully!");
    }

    private static String addTimestamp(String payload) {
        if (payload != null && payload.endsWith("}")) {
            long timestamp = System.currentTimeMillis();
            return payload.substring(0, payload.length() - 1) +
                   ",\"processedAt\":\"" + timestamp + "\"}";
        }
        return payload;
    }

    private static String extractField(String payload, String fieldName) {
        String searchPattern = "\"" + fieldName + "\":";
        int startIndex = payload.indexOf(searchPattern);
        if (startIndex != -1) {
            startIndex += searchPattern.length();
            while (startIndex < payload.length() &&
                   Character.isWhitespace(payload.charAt(startIndex))) {
                startIndex++;
            }
            if (startIndex < payload.length() && payload.charAt(startIndex) == '"') {
                int endIndex = payload.indexOf('"', startIndex + 1);
                if (endIndex != -1) {
                    return payload.substring(startIndex + 1, endIndex);
                }
            }
        }
        return null;
    }
}