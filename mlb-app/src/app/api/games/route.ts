import { NextResponse } from "next/server";
import fs from "fs";
import path from "path";
import csvParser from "csv-parser";

export async function GET() {
    try {
        // Move up one level from mlb-app to MLB-Analytics/picks
        const picksDir = path.resolve(process.cwd(), "../picks");

        // List all CSV files
        const files = fs.readdirSync(picksDir).filter(file => file.endsWith(".csv"));
        if (files.length === 0) return NextResponse.json({ error: "No games available" }, { status: 404 });

        // Get the latest file (Assumes most recent date in filename)
        const latestFile = files.sort().pop();
        if (!latestFile) return NextResponse.json({ error: "No game data found" }, { status: 404 });

        const filePath = path.join(picksDir, latestFile);
        const results: any[] = [];

        // Read CSV file and parse data
        await new Promise((resolve, reject) => {
            fs.createReadStream(filePath)
                .pipe(csvParser())
                .on("data", (row) => results.push(row))
                .on("end", resolve)
                .on("error", reject);
        });

        return NextResponse.json(results);
    } catch (error) {
        console.error("Error loading game data:", error);
        return NextResponse.json({ error: "Failed to load game data" }, { status: 500 });
    }
}
