// app/api/predictions/route.ts
import { NextResponse } from "next/server";
import { parse } from "csv-parse/sync";
import fs from "fs/promises";
import path from "path";

type Prediction = {
  GAME_ID: string;
  AWAY_NAME: string;
  HOME_NAME: string;
  PREDICTION: string;
  TIMESTAMP: string;
};

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const date = searchParams.get("date") || new Date().toISOString().split("T")[0];
    const baseUrl = process.env.VERCEL_URL ? `https://${process.env.VERCEL_URL}` : "http://localhost:3000";
    const filePath = path.join(process.cwd(), "public/data/predictions.csv");
    let fileContent: string;

    console.log(`Attempting to fetch predictions.csv from ${baseUrl}/data/predictions.csv`);
    try {
      const response = await fetch(`${baseUrl}/data/predictions.csv`);
      if (!response.ok) {
        console.error(`Fetch failed with status: ${response.status}`);
        throw new Error(`Fetch failed: ${response.status}`);
      }
      fileContent = await response.text();
    } catch (fetchError) {
      console.warn("Fetch failed, attempting to read from filesystem:", fetchError);
      try {
        fileContent = await fs.readFile(filePath, "utf-8");
        console.log(`Successfully read predictions.csv from ${filePath}`);
      } catch (fsError) {
        console.error(`Filesystem read failed for ${filePath}:`, fsError);
        throw new Error(`Failed to read predictions.csv from ${filePath}: ${fsError}`);
      }
    }

    if (!fileContent.trim()) {
      console.warn("predictions.csv is empty");
      return NextResponse.json([]);
    }

    const records = parse(fileContent, { columns: true, skip_empty_lines: true }) as Prediction[];
    const filteredPreds = records
      .filter((p: Prediction) => p.TIMESTAMP.startsWith(date))
      .map((p: Prediction) => ({
        ...p,
        PREDICTION: parseInt(p.PREDICTION, 10),
      }));
    console.log("Filtered predictions:", filteredPreds);
    return NextResponse.json(filteredPreds);
  } catch (error) {
    console.error("API Error:", error);
    const errorMessage = error instanceof Error ? error.message : "Unknown error occurred";
    return NextResponse.json({ error: `Failed to read predictions.csv: ${errorMessage}` }, { status: 500 });
  }
}
