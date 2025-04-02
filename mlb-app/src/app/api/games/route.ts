import { type NextRequest, NextResponse } from "next/server"
import fs from "fs"
import path from "path"
import { parse } from "csv-parse/sync"

export async function GET(request: NextRequest) {
  const searchParams = request.nextUrl.searchParams
  // Get current date in YYYY-MM-DD format
  const today = new Date().toISOString().split("T")[0]
  const date = searchParams.get("date") || today // Default to today's date if not provided

  try {
    // Format the date to match the CSV file naming convention
    const formattedDate = date.replace(/-/g, "-")

    // Look for the CSV file in the src/app/api/picks directory
    const filePath = path.join(process.cwd(), "src", "app", "api", "picks", `${formattedDate}.csv`)

    // Check if the file exists
    if (!fs.existsSync(filePath)) {
      return NextResponse.json({ error: "No data available for the selected date" }, { status: 404 })
    }

    // Read and parse the CSV file
    const fileContent = fs.readFileSync(filePath, "utf8")
    const records = parse(fileContent, {
      columns: true,
      skip_empty_lines: true,
      relax_column_count: true, // Handle potential inconsistencies in the CSV
    })

    return NextResponse.json(records)
  } catch (error) {
    console.error("Error processing request:", error)
    return NextResponse.json({ error: "Failed to process request" }, { status: 500 })
  }
}

// Download endpoint - POST method to download and save CSV files
export async function POST(request: NextRequest) {
  try {
    const { date, url } = await request.json()

    if (!url) {
      return NextResponse.json({ error: "URL parameter is required" }, { status: 400 })
    }

    // Format the date to match the CSV file naming convention
    const formattedDate = date.replace(/-/g, "-")

    // Define the path where the CSV will be saved
    const dirPath = path.join(process.cwd(), "src", "app", "api", "picks")
    const filePath = path.join(dirPath, `${formattedDate}.csv`)

    // Ensure the directory exists
    if (!fs.existsSync(dirPath)) {
      fs.mkdirSync(dirPath, { recursive: true })
    }

    // Fetch the CSV from the provided URL
    const response = await fetch(url)
    if (!response.ok) {
      throw new Error(`Failed to fetch from URL: ${response.status}`)
    }

    const csvData = await response.text()

    // Save the file
    fs.writeFileSync(filePath, csvData)

    return NextResponse.json({
      success: true,
      message: `CSV file for ${formattedDate} has been downloaded and saved.`,
      path: filePath,
    })
  } catch (error) {
    console.error("Error downloading CSV:", error)
    return NextResponse.json(
      {
        error: "Failed to download and save CSV file",
        details: error instanceof Error ? error.message : "Unknown error",
      },
      { status: 500 },
    )
  }
}

