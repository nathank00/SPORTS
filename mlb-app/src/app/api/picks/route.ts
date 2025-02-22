import { NextResponse } from "next/server";
import fs from "fs";
import path from "path";

export async function GET(req: Request) {
    try {
        const url = new URL(req.url);
        const dateParam = url.searchParams.get("date"); // Get date from query params

        // Default to today's date if no date is provided
        const selectedDate = dateParam || new Date().toISOString().split("T")[0];

        const picksDir = path.join(process.cwd(), "src/app/api/picks");
        const filePath = path.join(picksDir, `${selectedDate}.csv`);

        if (!fs.existsSync(filePath)) {
            return NextResponse.json({ error: `No picks available for ${selectedDate}.` }, { status: 404 });
        }

        const fileData = fs.readFileSync(filePath, "utf8");
        const lines = fileData.trim().split("\n");

        if (lines.length < 2) {
            return NextResponse.json({ error: `Pick file for ${selectedDate} exists but is empty.` }, { status: 204 });
        }

        const headers = lines[0].split(",");
        const games = lines.slice(1).map((line) => {
            const values = line.split(",");
            return headers.reduce((acc, header, index) => {
                acc[header.trim()] = values[index]?.trim() || null;
                return acc;
            }, {} as Record<string, string | null>);
        });

        return NextResponse.json(games);
    } catch (err: unknown) {
        let errorMessage = "An unknown error occurred.";
        if (err instanceof Error) {
            errorMessage = err.message;
        }
        console.error("Error in API route:", errorMessage);
        return NextResponse.json({ error: errorMessage }, { status: 500 });
    }
}
