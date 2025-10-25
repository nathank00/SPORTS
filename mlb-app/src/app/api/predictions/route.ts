import { NextResponse } from "next/server";
import { parse } from "csv-parse/sync";

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
    const date = searchParams.get("date") || "2025-10-25";
    const baseUrl = process.env.VERCEL_URL ? `https://${process.env.VERCEL_URL}` : 'http://localhost:3000';
    const response = await fetch(`${baseUrl}/data/predictions.csv`);
    if (!response.ok) throw new Error(`Fetch failed: ${response.status}`);
    const fileContent = await response.text();
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
