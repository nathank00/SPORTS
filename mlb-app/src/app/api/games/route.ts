import { NextResponse } from "next/server";
import fs from "fs";
import path from "path";

export async function GET() {
    try {
        const gamesDir = path.join(process.cwd(), "src/app/api/games");
        const today = new Date().toISOString().split("T")[0] + ".csv";
        const filePath = path.join(gamesDir, today);

        if (!fs.existsSync(filePath)) {
            return NextResponse.json({ error: "No game data available for today." }, { status: 404 });
        }

        const fileData = fs.readFileSync(filePath, "utf8");
        const lines = fileData.trim().split("\n");

        if (lines.length < 2) {
            return NextResponse.json({ error: "Game file exists but is empty." }, { status: 204 });
        }

        const headers = lines[0].split(",");
        const games = lines.slice(1).map((line) => {
            const values = line.split(",");
            return headers.reduce((acc: Record<string, string | null>, header, index) => {
                acc[header.trim()] = values[index]?.trim() || null;
                return acc;
            }, {});
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
