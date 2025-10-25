// app/api/predictions/route.ts
import { NextResponse } from "next/server";
import { promises as fs } from "fs";
import path from "path";
import { parse } from "csv-parse/sync";

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const date = searchParams.get("date") || "2025-10-25";
    const filePath = "/Users/njk/Development/SPORTS/NBA/model/predictions.csv";
    console.log(`Reading predictions.csv from: ${filePath} for date: ${date}`);
    const fileContent = await fs.readFile(filePath, "utf-8");
    const records = parse(fileContent, { columns: true, skip_empty_lines: true });
    const filteredPreds = records
      .filter((p: any) => p.TIMESTAMP.startsWith(date))
      .map((p: any) => ({
        ...p,
        PREDICTION: parseInt(p.PREDICTION, 10), // Ensure PREDICTION is a number
      }));
    console.log("Filtered predictions:", filteredPreds);
    return NextResponse.json(filteredPreds);
  } catch (error) {
    console.error("API Error:", error);
    const errorMessage = error instanceof Error ? error.message : "Unknown error occurred";
    return NextResponse.json({ error: `Failed to read predictions.csv: ${errorMessage}` }, { status: 500 });
  }
}
