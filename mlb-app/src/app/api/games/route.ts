import { NextResponse } from "next/server";
import fs from "fs";
import path from "path";

export async function GET() {
    try {
        // Define the directory where CSV files are stored
        const picksDirectory = path.join(process.cwd(), "picks");

        // Get the list of CSV files in the directory
        const files = fs.readdirSync(picksDirectory).filter(file => file.endsWith(".csv"));

        if (files.length === 0) {
            return NextResponse.json({ message: "No picks available" }, { status: 404 });
        }

        // Get the most recent file (assuming filenames are dates like '2024-06-01.csv')
        const latestFile = files.sort().reverse()[0];
        const filePath = path.join(picksDirectory, latestFile);

        // Read the CSV file
        const data = fs.readFileSync(filePath, "utf-8").trim();

        if (!data) {
            return NextResponse.json({ message: "CSV file is empty" }, { status: 500 });
        }

        // Convert CSV to JSON (basic parsing)
        const rows = data.split("\n").map(row => row.split(","));

        // Ensure headers exist
        if (rows.length < 2) {
            return NextResponse.json({ message: "Invalid CSV format" }, { status: 500 });
        }

        const headers = rows.shift();
        if (!headers || headers.length === 0) {
            return NextResponse.json({ message: "Missing headers in CSV" }, { status: 500 });
        }

        const jsonData = rows
            .filter(row => row.length === headers.length) // Ensure row matches headers
            .map(row => {
                return headers.reduce((obj, header, index) => {
                    obj[header.trim()] = row[index]?.trim() ?? "";
                    return obj;
                }, {} as Record<string, string>);
            });

        return NextResponse.json(jsonData, { status: 200 });

    } catch (error) {
        console.error("API Error:", error);
        return NextResponse.json({ message: "Internal Server Error" }, { status: 500 });
    }
}
